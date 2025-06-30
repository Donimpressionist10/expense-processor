package org.finance.expenseprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.math.BigDecimal;
import java.math.RoundingMode;
import com.amazonaws.services.s3.model.ObjectMetadata;

public class ExpenseProcessor implements RequestHandler<S3Event, String> {
    
    private static final Logger logger = LoggerFactory.getLogger(ExpenseProcessor.class);
    private final AmazonS3 s3Client;
    private static final String FILTER_CONFIG_KEY = "filter-config.txt";
    private Set<String> filterPatterns;
    
    // Java 21 record for column mapping
    public record ColumnIndices(int valueDate, int description, int amount) {
        public boolean isValid() {
            return valueDate != -1 && description != -1 && amount != -1;
        }
    }
    
    // Java 21 record for expense data
    public record ExpenseRecord(String valueDate, String description, String amount) {
        public String[] toArray() {
            return new String[]{valueDate, description, amount};
        }
        
        public BigDecimal getAmountAsBigDecimal() {
            try {
                return new BigDecimal(amount);
            } catch (NumberFormatException e) {
                logger.warn("Could not parse amount '{}' as number, using 0.00", amount);
                return BigDecimal.ZERO;
            }
        }
    }
    
    // Java 21 record for collapsed expense data
    public record CollapsedExpenseRecord(Set<String> valueDates, String description, BigDecimal totalAmount) {
        public String[] toArray() {
            // Always use the latest date for simplicity and cleanliness
            var latestDate = valueDates.stream()
                    .max(String::compareTo)
                    .orElse("");
            
            return new String[]{latestDate, description, totalAmount.setScale(2, RoundingMode.HALF_UP).toString()};
        }
        
        public static CollapsedExpenseRecord fromSingle(ExpenseRecord record) {
            return new CollapsedExpenseRecord(
                    Set.of(record.valueDate()),
                    record.description(),
                    record.getAmountAsBigDecimal()
            );
        }
        
        public CollapsedExpenseRecord addRecord(ExpenseRecord record) {
            var newDates = new HashSet<>(valueDates);
            newDates.add(record.valueDate());
            return new CollapsedExpenseRecord(
                    newDates,
                    description,
                    totalAmount.add(record.getAmountAsBigDecimal())
            );
        }
    }
    
    public ExpenseProcessor() {
        this.s3Client = AmazonS3ClientBuilder.defaultClient();
        this.filterPatterns = new HashSet<>();
    }
    
    public ExpenseProcessor(AmazonS3 s3Client) {
        this.s3Client = s3Client;
        this.filterPatterns = new HashSet<>();
    }
    
    @Override
    public String handleRequest(S3Event event, Context context) {
        logger.info("Processing S3 event with {} records", event.getRecords().size());
        
        // Modern Java 21 - using streams for processing
        var processedCount = event.getRecords().stream()
                .mapToInt(this::processRecord)
                .sum();
        
        return "Successfully processed " + processedCount + " of " + event.getRecords().size() + " records";
    }
    
    private int processRecord(S3EventNotification.S3EventNotificationRecord record) {
        var bucketName = record.getS3().getBucket().getName();
        var objectKey = record.getS3().getObject().getKey();
        
        logger.info("Processing file: s3://{}/{}", bucketName, objectKey);
        
        try {
            processS3Object(bucketName, objectKey);
            return 1;
        } catch (Exception e) {
            logger.error("Error processing S3 object: s3://{}/{}", bucketName, objectKey, e);
            return 0;
        }
    }
    
    private void processS3Object(String bucketName, String objectKey) throws Exception {
        logger.info("Reading S3 object: s3://{}/{}", bucketName, objectKey);
        
        try (var s3Object = s3Client.getObject(bucketName, objectKey);
             var reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
            
            var metadata = s3Object.getObjectMetadata();
            logger.info("Content-Type: {}, Content-Length: {}", 
                       metadata.getContentType(), metadata.getContentLength());
            
            // Read the entire email content properly
            var emailContent = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                emailContent.append(line).append("\n");
            }
            var emailString = emailContent.toString();
            
            // Extract and process CSV content
            extractCsvFromEmail(emailString)
                    .ifPresentOrElse(
                            csvContent -> {
                                try {
                                    processCsvContent(csvContent, bucketName, objectKey);
                                } catch (Exception e) {
                                    logger.error("Error processing CSV content", e);
                                }
                            },
                            () -> logger.warn("No CSV content found in email")
                    );
        }
    }
    
    private Optional<String> extractCsvFromEmail(String emailContent) {
        try {
            var lines = Arrays.stream(emailContent.split("\n")).toList();
            logger.info("Email has {} lines total", lines.size());
            
            // Debug: Look for Content-Transfer-Encoding patterns
            var encodingLines = lines.stream()
                    .filter(line -> line.toLowerCase().contains("content-transfer-encoding"))
                    .toList();
            logger.info("Found {} Content-Transfer-Encoding lines: {}", encodingLines.size(), encodingLines);
            
            // Find the start of base64 section using modern stream processing
            var base64StartIndex = IntStream.range(0, lines.size())
                    .filter(i -> lines.get(i).trim().equals("Content-Transfer-Encoding: base64"))
                    .findFirst();
            
            if (base64StartIndex.isEmpty()) {
                logger.warn("No 'Content-Transfer-Encoding: base64' line found");
                return Optional.empty();
            }
            
            logger.info("Found base64 section starting at line {}", base64StartIndex.getAsInt());
            
            // Extract base64 content using streams
            var base64Lines = lines.stream()
                    .skip(base64StartIndex.getAsInt() + 1)
                    .takeWhile(line -> !line.startsWith("--") || !line.contains("--"))
                    .filter(line -> !line.trim().isEmpty() && 
                                   !line.startsWith("Content-ID:") && 
                                   !line.startsWith("X-Attachment-Id:"))
                    .toList();
            
            logger.info("Collected {} base64 lines", base64Lines.size());
            
            var base64Content = base64Lines.stream()
                    .map(String::trim)
                    .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                    .toString();
            
            if (base64Content.isEmpty()) {
                logger.warn("No base64 content collected");
                return Optional.empty();
            }
            
            logger.info("Base64 content length: {}", base64Content.length());
            return Optional.of(new String(Base64.getDecoder().decode(base64Content)));
                    
        } catch (Exception e) {
            logger.error("Error extracting CSV from email", e);
            return Optional.empty();
        }
    }
    
    // Package-private for testing
    void processCsvContent(String csvContent, String bucketName, String originalObjectKey) throws Exception {
        logger.info("Processing CSV content");
        
        try (var csvReader = new CSVReader(new StringReader(csvContent))) {
            var records = csvReader.readAll();
            logger.info("CSV contains {} rows", records.size());
            
            if (records.isEmpty()) {
                logger.warn("No records found in CSV");
                return;
            }
            
            // Find column indices using modern Optional pattern
            var columnIndices = findColumnIndices(records.get(0));
            if (!columnIndices.isValid()) {
                logger.error("Required columns not found: {}", columnIndices);
                return;
            }
            
            // Load filter patterns before processing
            loadFilterPatterns(bucketName);
            
            // Process CSV data using streams and records with filtering
            var expenseRecords = records.stream()
                    .skip(1) // Skip header
                    .filter(row -> row.length > Math.max(columnIndices.valueDate(), 
                                                       Math.max(columnIndices.description(), columnIndices.amount())))
                    .map(row -> new ExpenseRecord(
                            row[columnIndices.valueDate()],
                            row[columnIndices.description()],
                            row[columnIndices.amount()]
                    ))
                    .filter(record -> !shouldFilterRecord(record))
                    .filter(record -> !hasPositiveAmount(record))
                    .peek(record -> logger.info("Included CSV row: [{}, {}, {}]", 
                                               record.valueDate(), record.description(), record.amount()))
                    .toList();
            
            logger.info("After filtering: {} records remaining out of {} original records", 
                       expenseRecords.size(), records.size() - 1);
            
            // Collapse records with same description
            var collapsedRecords = collapseRecords(expenseRecords);
            
            // Write collapsed CSV to S3
            writeCollapsedCsvToS3(collapsedRecords, bucketName, originalObjectKey);
        }
    }
    
    private ColumnIndices findColumnIndices(String[] headers) {
        // Modern Java - using streams to find column indices
        var headersList = Arrays.asList(headers);
        
        var valueDateIndex = IntStream.range(0, headers.length)
                .filter(i -> headers[i].trim().equalsIgnoreCase("Value Date"))
                .findFirst().orElse(-1);
                
        var descriptionIndex = IntStream.range(0, headers.length)
                .filter(i -> headers[i].trim().equalsIgnoreCase("Description"))
                .findFirst().orElse(-1);
                
        var amountIndex = IntStream.range(0, headers.length)
                .filter(i -> headers[i].trim().equalsIgnoreCase("Amount"))
                .findFirst().orElse(-1);
        
        return new ColumnIndices(valueDateIndex, descriptionIndex, amountIndex);
    }
    
    private void writeReducedCsvToS3(List<ExpenseRecord> expenseRecords, String bucketName, String originalObjectKey) throws Exception {
        logger.info("Writing reduced CSV to S3");
        
        // Create CSV content with header + data rows
        var allRecords = new ArrayList<String[]>();
        allRecords.add(new String[]{"Value Date", "Description", "Amount"});
        expenseRecords.stream()
                .map(ExpenseRecord::toArray)
                .forEach(allRecords::add);
        
        // Generate CSV content using try-with-resources
        var csvContent = generateCsvContent(allRecords);
        var outputKey = "processed/" + extractFileNameFromKey(originalObjectKey) + "_processed.csv";
        
        // Upload to S3 with modern var declarations
        var csvBytes = csvContent.getBytes();
        var metadata = new ObjectMetadata();
        metadata.setContentLength(csvBytes.length);
        metadata.setContentType("text/csv");
        
        try (var inputStream = new ByteArrayInputStream(csvBytes)) {
            s3Client.putObject(bucketName, outputKey, inputStream, metadata);
            logger.info("Successfully wrote reduced CSV to: s3://{}/{}", bucketName, outputKey);
            logger.info("Reduced CSV contains {} expense records", expenseRecords.size());
        }
    }
    
    private String generateCsvContent(List<String[]> records) throws Exception {
        try (var stringWriter = new StringWriter();
             var csvWriter = new CSVWriter(stringWriter, 
                     CSVWriter.DEFAULT_SEPARATOR, 
                     CSVWriter.DEFAULT_QUOTE_CHARACTER,
                     CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                     CSVWriter.DEFAULT_LINE_END)) {
            csvWriter.writeAll(records);
            return stringWriter.toString();
        }
    }
    
    private String extractFileNameFromKey(String objectKey) {
        // Modern Java - using switch expression for cleaner logic
        var fileName = objectKey.substring(objectKey.lastIndexOf('/') + 1);
        var dotIndex = fileName.lastIndexOf('.');
        
        return switch (dotIndex) {
            case -1, 0 -> fileName;  // No extension or starts with dot
            default -> fileName.substring(0, dotIndex);
        };
    }
    
    private void loadFilterPatterns(String bucketName) {
        try {
            logger.info("Loading filter patterns from s3://{}/{}", bucketName, FILTER_CONFIG_KEY);
            
            var filterObject = s3Client.getObject(bucketName, FILTER_CONFIG_KEY);
            try (var reader = new BufferedReader(new InputStreamReader(filterObject.getObjectContent()))) {
                filterPatterns = reader.lines()
                        .map(String::trim)
                        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                        .collect(HashSet::new, HashSet::add, HashSet::addAll);
                
                logger.info("Loaded {} filter patterns: {}", filterPatterns.size(), filterPatterns);
            } finally {
                filterObject.close();
            }
        } catch (Exception e) {
            logger.warn("Could not load filter patterns from s3://{}/{}: {}. Processing without filtering.", 
                       bucketName, FILTER_CONFIG_KEY, e.getMessage());
            filterPatterns = new HashSet<>();
        }
    }
    
    private boolean shouldFilterRecord(ExpenseRecord record) {
        var description = record.description().toLowerCase();
        
        var shouldFilter = filterPatterns.stream()
                .anyMatch(pattern -> description.contains(pattern.toLowerCase()));
        
        if (shouldFilter) {
            logger.info("Filtering out record with description: {}", record.description());
        }
        
        return shouldFilter;
    }
    
    private boolean hasPositiveAmount(ExpenseRecord record) {
        try {
            var amount = record.getAmountAsBigDecimal();
            var isPositive = amount.compareTo(BigDecimal.ZERO) > 0;
            
            if (isPositive) {
                logger.info("Filtering out record with positive amount: {} ({})", 
                           record.description(), amount);
            }
            
            return isPositive;
        } catch (Exception e) {
            logger.warn("Could not parse amount '{}' for filtering, including record", record.amount());
            return false;
        }
    }
    
    // Package-private for testing
    List<CollapsedExpenseRecord> collapseRecords(List<ExpenseRecord> expenseRecords) {
        logger.info("Collapsing {} records by description", expenseRecords.size());
        
        // Group by normalized description and collect into CollapsedExpenseRecord
        var collapsedMap = expenseRecords.stream()
                .collect(Collectors.groupingBy(record -> normalizeDescription(record.description())))
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                            var records = entry.getValue();
                            var normalizedDescription = entry.getKey();
                            var dates = records.stream()
                                    .map(ExpenseRecord::valueDate)
                                    .collect(Collectors.toSet());
                            var totalAmount = records.stream()
                                    .map(ExpenseRecord::getAmountAsBigDecimal)
                                    .reduce(BigDecimal.ZERO, BigDecimal::add);
                            
                            return new CollapsedExpenseRecord(dates, normalizedDescription, totalAmount);
                        }
                ));
        
        var collapsedRecords = collapsedMap.values().stream()
                .sorted((r1, r2) -> r1.description().compareToIgnoreCase(r2.description()))
                .toList();
        
        logger.info("Collapsed to {} unique descriptions", collapsedRecords.size());
        
        // Log collapsed records for debugging
        collapsedRecords.forEach(record -> 
                logger.info("Collapsed record: {} | {} | {} (dates: {})", 
                           record.description(), 
                           record.totalAmount().setScale(2, RoundingMode.HALF_UP),
                           record.valueDates().size() > 1 ? "COLLAPSED" : "SINGLE",
                           String.join(", ", record.valueDates().stream().sorted().toList())
                )
        );
        
        return collapsedRecords;
    }
    
    private String normalizeDescription(String description) {
        var normalized = description.trim().toUpperCase();
        
        // Define normalization patterns - group similar merchants
        var patterns = new HashMap<String, String>();
        patterns.put("UBER", "Uber");
        patterns.put("WOOLWORTHS", "Woolworths");
        patterns.put("EMPACT", "Empact Amazon");
        patterns.put("AMAZON", "Amazon");
        patterns.put("APPLE.COM", "Apple");
        patterns.put("ITUNES", "Apple");
        patterns.put("STEAM", "Steam");
        patterns.put("NINTENDO", "Nintendo");
        patterns.put("GOOGLE", "Google");
        patterns.put("PAYSTACK", "PayStack");
        patterns.put("CHECKERS", "Checkers");
        patterns.put("TAKEALO", "TakeALot");
        patterns.put("DISCOVERY CARD PAYMENT", "Discovery Card Payment");
        patterns.put("MONTHLY ACCOUNT FEE", "Monthly Account Fee");
        patterns.put("VITALITY", "Vitality");
        patterns.put("PAYFAST", "PayFast");
        
        // Find the first matching pattern and return the normalized name
        for (var entry : patterns.entrySet()) {
            if (normalized.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        
        // For transactions without specific patterns, use the original description
        return description;
    }
    
    private void writeCollapsedCsvToS3(List<CollapsedExpenseRecord> collapsedRecords, String bucketName, String originalObjectKey) throws Exception {
        logger.info("Writing collapsed CSV to S3");
        
        // Create CSV content with header + data rows
        var allRecords = new ArrayList<String[]>();
        allRecords.add(new String[]{"Value Dates", "Description", "Total Amount"});
        collapsedRecords.stream()
                .map(CollapsedExpenseRecord::toArray)
                .forEach(allRecords::add);
        
        // Generate CSV content using try-with-resources
        var csvContent = generateCsvContent(allRecords);
        var outputKey = "processed/" + extractFileNameFromKey(originalObjectKey) + "_collapsed.csv";
        
        // Upload to S3 with modern var declarations
        var csvBytes = csvContent.getBytes();
        var metadata = new ObjectMetadata();
        metadata.setContentLength(csvBytes.length);
        metadata.setContentType("text/csv");
        
        try (var inputStream = new ByteArrayInputStream(csvBytes)) {
            s3Client.putObject(bucketName, outputKey, inputStream, metadata);
            logger.info("Successfully wrote collapsed CSV to: s3://{}/{}", bucketName, outputKey);
            logger.info("Collapsed CSV contains {} unique expense descriptions", collapsedRecords.size());
        }
    }
}
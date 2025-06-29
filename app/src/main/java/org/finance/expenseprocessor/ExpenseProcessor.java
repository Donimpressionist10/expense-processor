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
    
    private void processCsvContent(String csvContent, String bucketName, String originalObjectKey) throws Exception {
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
                    .peek(record -> logger.info("Included CSV row: [{}, {}, {}]", 
                                               record.valueDate(), record.description(), record.amount()))
                    .toList();
            
            logger.info("After filtering: {} records remaining out of {} original records", 
                       expenseRecords.size(), records.size() - 1);
            
            // Write reduced CSV to S3
            writeReducedCsvToS3(expenseRecords, bucketName, originalObjectKey);
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
             var csvWriter = new CSVWriter(stringWriter)) {
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
}
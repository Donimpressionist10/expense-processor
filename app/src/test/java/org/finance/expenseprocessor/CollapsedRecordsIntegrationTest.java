package org.finance.expenseprocessor;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.amazonaws.services.s3.AmazonS3;
import com.opencsv.CSVReader;

class CollapsedRecordsIntegrationTest {
    
    private AmazonS3 mockS3Client;
    private ExpenseProcessor processor;
    private String testCsvContent;
    
    @BeforeEach
    void setUp() throws IOException {
        mockS3Client = Mockito.mock(AmazonS3.class);
        processor = new ExpenseProcessor(mockS3Client);
        
        // Load test CSV content
        var testCsvPath = Paths.get("../test.csv");
        if (!Files.exists(testCsvPath)) {
            testCsvPath = Paths.get("../../test.csv");
        }
        testCsvContent = Files.readString(testCsvPath);
    }
    
    @Test
    void testCollapsingLogicWithRealData() throws Exception {
        // Parse the test CSV into ExpenseRecords
        var expenseRecords = parseTestCsvToExpenseRecords(testCsvContent);
        
        // Test the collapsing logic directly
        var collapsedRecords = processor.collapseRecords(expenseRecords);
        
        // Verify the collapsed records
        verifyCollapsedRecords(collapsedRecords);
        
        // Verify specific scenarios from our test data
        verifySpecificCollapses(collapsedRecords);
    }
    
    private List<ExpenseProcessor.ExpenseRecord> parseTestCsvToExpenseRecords(String csvContent) throws Exception {
        try (var reader = new CSVReader(new StringReader(csvContent))) {
            var allRows = reader.readAll();
            if (allRows.size() < 2) return List.of();
            
            // Skip header row and parse into ExpenseRecords
            return allRows.stream()
                    .skip(1)
                    .map(row -> new ExpenseProcessor.ExpenseRecord(
                            row[0], // Value Date
                            row[3], // Description  
                            row[5]  // Amount
                    ))
                    .toList();
        }
    }
    
    @Test
    void testSpecificCollapsingScenarios() throws Exception {
        // Test with a simplified dataset to verify specific collapsing logic
        var simpleCsv = """
            "Value Date","Value Time","Type","Description","Beneficiary or CardHolder","Amount"
            2025-06-28,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-91.00
            2025-06-27,08:26:53,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-104.00
            2025-06-26,08:06:04,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-25,21:16:20,"POS Purchase","WOOLWORTHS CAPE TOWN","J Daniels",-142.07
            2025-06-24,21:24:43,"Apple Pay","WOOLWORTHS CAPE TOWN","J Daniels",-584.96
            2025-06-23,21:45:21,"Apple Pay","WOOLWORTHS CAPE TOWN","J Daniels",-269.99
            """;
        
        // Parse simple CSV and test collapsing directly
        var expenseRecords = parseTestCsvToExpenseRecords(simpleCsv);
        var collapsedRecords = processor.collapseRecords(expenseRecords);
        
        // Should have exactly 2 collapsed records: Uber and Woolworths
        assertEquals(2, collapsedRecords.size(), "Should have exactly 2 collapsed records");
        
        // Find Uber record
        var uberRecord = collapsedRecords.stream()
                .filter(r -> r.description().contains("Uber"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Uber record not found"));
        
        assertEquals(new BigDecimal("-205.00"), uberRecord.totalAmount(), "Uber total should be -205.00 (-91 + -104 + -10)");
        assertTrue(uberRecord.valueDates().contains("2025-06-28"), "Should contain first Uber date");
        assertTrue(uberRecord.valueDates().contains("2025-06-27"), "Should contain second Uber date");
        assertTrue(uberRecord.valueDates().contains("2025-06-26"), "Should contain third Uber date");
        
        // Verify that the CSV output uses only the latest date
        var uberArray = uberRecord.toArray();
        assertEquals("2025-06-28", uberArray[0], "Should use latest date in CSV output");
        
        // Find Woolworths record
        var woolworthsRecord = collapsedRecords.stream()
                .filter(r -> r.description().contains("Woolworths"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Woolworths record not found"));
        
        assertEquals(new BigDecimal("-997.02"), woolworthsRecord.totalAmount(), "Woolworths total should be -997.02 (-142.07 + -584.96 + -269.99)");
        assertTrue(woolworthsRecord.valueDates().contains("2025-06-25"), "Should contain first Woolworths date");
        assertTrue(woolworthsRecord.valueDates().contains("2025-06-24"), "Should contain second Woolworths date");
        assertTrue(woolworthsRecord.valueDates().contains("2025-06-23"), "Should contain third Woolworths date");
        
        // Verify that the CSV output uses only the latest date
        var woolworthsArray = woolworthsRecord.toArray();
        assertEquals("2025-06-25", woolworthsArray[0], "Should use latest Woolworths date in CSV output");
    }
    
    @Test
    void testDescriptionNormalization() throws Exception {
        // Test that different variations of the same merchant get normalized properly
        var normalizationCsv = """
            "Value Date","Value Time","Type","Description","Beneficiary or CardHolder","Amount"
            2025-06-28,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-91.00
            2025-06-27,08:26:53,"Pending","Uber CAPE TOWN","Jake Daniels",-104.00
            2025-06-26,08:06:04,"Pending","UBER EATS JOHANNESBURG","Jake Daniels",-10.00
            2025-06-25,21:16:20,"POS Purchase","WOOLWORTHS CAPE TOWN","J Daniels",-142.07
            2025-06-24,21:24:43,"Apple Pay","WOOLWORTHS OBSERVATORY","J Daniels",-584.96
            2025-06-23,21:45:21,"Apple Pay","APPLE.COM/BILL ITUNES.COM","J Daniels",-269.99
            2025-06-22,21:45:21,"Apple Pay","ITUNES STORE","J Daniels",-50.00
            """;
        
        var expenseRecords = parseTestCsvToExpenseRecords(normalizationCsv);
        var collapsedRecords = processor.collapseRecords(expenseRecords);
        
        // Should have exactly 3 collapsed records: Uber, Woolworths, Apple
        assertEquals(3, collapsedRecords.size(), "Should have exactly 3 collapsed records after normalization");
        
        // Verify Uber normalization (all Uber variants should be grouped)
        var uberRecord = collapsedRecords.stream()
                .filter(r -> r.description().equals("Uber"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Normalized Uber record not found"));
        
        assertEquals(3, uberRecord.valueDates().size(), "All 3 Uber transactions should be grouped");
        assertEquals(new BigDecimal("-205.00"), uberRecord.totalAmount(), "Total Uber amount should be correct");
        
        // Verify Woolworths normalization
        var woolworthsRecord = collapsedRecords.stream()
                .filter(r -> r.description().equals("Woolworths"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Normalized Woolworths record not found"));
        
        assertEquals(2, woolworthsRecord.valueDates().size(), "Both Woolworths transactions should be grouped");
        assertEquals(new BigDecimal("-727.03"), woolworthsRecord.totalAmount(), "Total Woolworths amount should be correct");
        
        // Verify Apple normalization (Apple.com and iTunes should be grouped)
        var appleRecord = collapsedRecords.stream()
                .filter(r -> r.description().equals("Apple"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Normalized Apple record not found"));
        
        assertEquals(2, appleRecord.valueDates().size(), "Both Apple transactions should be grouped");
        assertEquals(new BigDecimal("-319.99"), appleRecord.totalAmount(), "Total Apple amount should be correct");
    }
    
    @Test
    void testDateFormatting() throws Exception {
        // Test different date formatting scenarios
        var manyDatesCsv = """
            "Value Date","Value Time","Type","Description","Beneficiary or CardHolder","Amount"
            2025-06-01,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-02,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-03,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-04,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-05,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-06,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-07,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            2025-06-08,12:45:56,"Pending","Uber JOHANNESBURG ZA","Jake Daniels",-10.00
            """;
        
        var expenseRecords = parseTestCsvToExpenseRecords(manyDatesCsv);
        var collapsedRecords = processor.collapseRecords(expenseRecords);
        
        assertEquals(1, collapsedRecords.size(), "Should have 1 collapsed Uber record");
        
        var uberRecord = collapsedRecords.get(0);
        assertEquals("Uber", uberRecord.description());
        assertEquals(8, uberRecord.valueDates().size());
        assertEquals(new BigDecimal("-80.00"), uberRecord.totalAmount());
        
        // Check that the date field uses just the latest date for many dates
        var datesArray = uberRecord.toArray();
        var datesField = datesArray[0];
        assertEquals("2025-06-08", datesField, "Should use only the latest date for many transactions");
        
        System.out.println("Date field format for 8 dates: " + datesField);
    }
    
    private void verifyCollapsedRecords(List<ExpenseProcessor.CollapsedExpenseRecord> records) {
        // Verify we have some collapsed records
        assertTrue(records.size() > 0, "Should have collapsed records");
        
        // Log results for debugging
        System.out.println("Collapsed Records Summary:");
        records.forEach(record -> {
            System.out.printf("- %s: %s (dates: %s)%n", 
                    record.description(), 
                    record.totalAmount(),
                    String.join(", ", record.valueDates()));
        });
        
        // Basic validations
        for (var record : records) {
            assertNotNull(record.description(), "Description should not be null");
            assertNotNull(record.totalAmount(), "Total amount should not be null");
            assertNotNull(record.valueDates(), "Value dates should not be null");
            assertFalse(record.valueDates().isEmpty(), "Should have at least one date");
        }
    }
    
    private void verifySpecificCollapses(List<ExpenseProcessor.CollapsedExpenseRecord> records) {
        // Verify Uber transactions were collapsed
        var uberRecords = records.stream()
                .filter(r -> r.description().toLowerCase().contains("uber"))
                .toList();
        
        if (!uberRecords.isEmpty()) {
            System.out.println("\nUber Records Analysis:");
            for (var uberRecord : uberRecords) {
                System.out.printf("- %s: %s (from %d dates: %s)%n", 
                        uberRecord.description(),
                        uberRecord.totalAmount(),
                        uberRecord.valueDates().size(),
                        String.join(", ", uberRecord.valueDates().stream().sorted().toList()));
                
                assertTrue(uberRecord.totalAmount().compareTo(BigDecimal.ZERO) < 0, 
                          "Uber expenses should be negative");
            }
        }
        
        // Verify Empact Amazon records
        var empactRecords = records.stream()
                .filter(r -> r.description().toLowerCase().contains("empact") && 
                           r.description().toLowerCase().contains("amazon"))
                .toList();
        
        if (!empactRecords.isEmpty()) {
            System.out.println("\nEmpact Amazon Records Analysis:");
            for (var empactRecord : empactRecords) {
                System.out.printf("- %s: %s (from %d dates)%n", 
                        empactRecord.description(),
                        empactRecord.totalAmount(),
                        empactRecord.valueDates().size());
            }
        }
        
        // Verify WOOLWORTHS records  
        var woolworthsRecords = records.stream()
                .filter(r -> r.description().toLowerCase().contains("woolworths"))
                .toList();
        
        if (!woolworthsRecords.isEmpty()) {
            System.out.println("\nWoolworths Records Analysis:");
            for (var woolworthsRecord : woolworthsRecords) {
                System.out.printf("- %s: %s (from %d dates)%n", 
                        woolworthsRecord.description(),
                        woolworthsRecord.totalAmount(),
                        woolworthsRecord.valueDates().size());
            }
        }
    }
}
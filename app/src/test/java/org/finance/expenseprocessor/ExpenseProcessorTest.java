package org.finance.expenseprocessor;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import com.amazonaws.services.s3.AmazonS3;
import static org.junit.jupiter.api.Assertions.*;

class ExpenseProcessorTest {
    @Test void testExpenseProcessorCreation() {
        AmazonS3 mockS3Client = Mockito.mock(AmazonS3.class);
        ExpenseProcessor processor = new ExpenseProcessor(mockS3Client);
        assertNotNull(processor);
    }
    
    @Test void testExpenseRecordCreation() {
        var record = new ExpenseProcessor.ExpenseRecord("2025-06-28", "Coffee Shop", "-4.95");
        assertEquals("2025-06-28", record.valueDate());
        assertEquals("Coffee Shop", record.description());
        assertEquals("-4.95", record.amount());
        
        String[] array = record.toArray();
        assertEquals(3, array.length);
        assertEquals("2025-06-28", array[0]);
        assertEquals("Coffee Shop", array[1]);
        assertEquals("-4.95", array[2]);
    }
    
    @Test void testColumnIndicesValidation() {
        var validIndices = new ExpenseProcessor.ColumnIndices(0, 1, 2);
        assertTrue(validIndices.isValid());
        
        var invalidIndices = new ExpenseProcessor.ColumnIndices(-1, 1, 2);
        assertFalse(invalidIndices.isValid());
    }
    
    @Test void testCollapsedExpenseRecord() {
        var record1 = new ExpenseProcessor.ExpenseRecord("2025-06-28", "Coffee Shop", "-4.95");
        var record2 = new ExpenseProcessor.ExpenseRecord("2025-06-29", "Coffee Shop", "-3.50");
        
        var collapsed = ExpenseProcessor.CollapsedExpenseRecord.fromSingle(record1);
        assertEquals(1, collapsed.valueDates().size());
        assertTrue(collapsed.valueDates().contains("2025-06-28"));
        assertEquals("Coffee Shop", collapsed.description());
        assertEquals("-4.95", collapsed.totalAmount().toString());
        
        var updated = collapsed.addRecord(record2);
        assertEquals(2, updated.valueDates().size());
        assertTrue(updated.valueDates().contains("2025-06-28"));
        assertTrue(updated.valueDates().contains("2025-06-29"));
        assertEquals("Coffee Shop", updated.description());
        assertEquals("-8.45", updated.totalAmount().toString());
        
        String[] array = updated.toArray();
        assertEquals(3, array.length);
        assertEquals("2025-06-29", array[0]); // Should use latest date only
        assertEquals("Coffee Shop", array[1]);
        assertEquals("-8.45", array[2]);
    }
}

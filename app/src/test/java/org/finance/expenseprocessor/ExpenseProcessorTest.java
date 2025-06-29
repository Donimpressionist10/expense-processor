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
}

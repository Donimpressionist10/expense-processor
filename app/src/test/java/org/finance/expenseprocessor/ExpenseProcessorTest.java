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
}

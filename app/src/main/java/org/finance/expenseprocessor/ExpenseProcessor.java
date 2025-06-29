package org.finance.expenseprocessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

public class ExpenseProcessor implements RequestHandler<S3Event, String> {
    
    private static final Logger logger = LoggerFactory.getLogger(ExpenseProcessor.class);
    
    @Override
    public String handleRequest(S3Event event, Context context) {
        logger.info("Hello from Lambda! Processing S3 event with {} records", event.getRecords().size());
        
        for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {
            String bucketName = record.getS3().getBucket().getName();
            String objectKey = record.getS3().getObject().getKey();
            
            logger.info("Processing file: s3://{}/{}", bucketName, objectKey);
            
            // TODO: Add CSV processing logic here
            // For now, just log the details
            if (objectKey.endsWith(".csv")) {
                logger.info("Found CSV file - ready for processing!");
            }
        }
        
        return "Successfully processed " + event.getRecords().size() + " records";
    }
}
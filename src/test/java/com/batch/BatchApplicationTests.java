package com.batch;

import com.batch.batch.SettlementJob;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

@SpringBootTest(classes = {SettlementJob.class, TestConfig.class})
@RunWith(SpringRunner.class)
@SpringBatchTest
@TestPropertySource(locations = "classpath:application-test.properties")
class BatchApplicationTests {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void job_test() throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("job.name", "SettlementJob v=1")
            .toJobParameters();

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        Assert.assertEquals(jobExecution.getStatus(), BatchStatus.COMPLETED);
    }
}

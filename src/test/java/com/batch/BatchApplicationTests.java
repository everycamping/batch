package com.batch;

import com.batch.batch.SettlementJob;
import com.batch.domain.order.OrderProduct;
import com.batch.domain.order.OrderProductRepository;
import com.batch.domain.order.OrderStatus;
import com.batch.domain.product.Product;
import com.batch.domain.product.ProductRepository;
import com.batch.domain.seller.Seller;
import com.batch.domain.seller.SellerRepository;
import com.batch.domain.settlement.DailySettlement;
import com.batch.domain.settlement.DailySettlementRepository;
import java.time.LocalDateTime;
import java.util.List;
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
    @Autowired
    private OrderProductRepository orderProductRepository;
    @Autowired
    private ProductRepository productRepository;
    @Autowired
    private SellerRepository sellerRepository;
    @Autowired
    DailySettlementRepository dailySettlementRepository;

    @Test
    public void job_test() throws Exception {

        //given
        setDateSales();
        JobParameters jobParameters = new JobParametersBuilder()
            .addString("job.name", "SettlementJob v=1")
            .toJobParameters();

        //when
        JobExecution jobExecution = jobLauncherTestUtils.launchJob(jobParameters);

        //then
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        List<DailySettlement> list = dailySettlementRepository.findAll();
        Assert.assertEquals(2, list.size());
        Assert.assertEquals(500 *2, list.get(0).getAmount().longValue());
        Assert.assertEquals(700, list.get(1).getAmount().longValue());
    }


    private void setDateSales() {

        Seller seller1 = sellerRepository.save(Seller.builder().build());
        Seller seller2 = sellerRepository.save(Seller.builder().build());

        Product product1 = productRepository.save(Product.builder()
                            .seller(seller1)
                            .build());
        Integer product1Amount = 500;

        Product product2 = productRepository.save(Product.builder()
            .seller(seller2)
            .build());
        Integer product2Amount = 700;

        orderProductRepository.save(OrderProduct.builder()
                .amount(product1Amount)
                .product(product1)
                .status(OrderStatus.CONFIRM)
                .confirmedAt(LocalDateTime.now().minusDays(1))
                .build());

        orderProductRepository.save(OrderProduct.builder()
            .amount(product1Amount)
            .product(product1)
            .status(OrderStatus.CONFIRM)
            .confirmedAt(LocalDateTime.now().minusDays(1))
            .build());

        orderProductRepository.save(OrderProduct.builder()
            .amount(product2Amount)
            .product(product2)
            .status(OrderStatus.CONFIRM)
            .confirmedAt(LocalDateTime.now().minusDays(1))
            .build());
    }

}

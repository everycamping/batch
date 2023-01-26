package com.batch.batch;

import com.batch.domain.order.OrderProduct;
import com.batch.domain.order.OrderStatus;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import javax.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class SettlementJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;


    private final int chunkSize=10;


    @Bean
    public Job SettlementJobBuild() {
        return jobBuilderFactory.get("SettlementJob")
            .start(SettlementJobStep1())
            .build();
    }

    @Bean
    public Step SettlementJobStep1() {
        return stepBuilderFactory.get("SettlementJob_step1")
            .<OrderProduct, OrderProduct>chunk(chunkSize)
            .reader(OrderProductItemReader())
            .processor(settlementProcessor())
            .writer(OrderProductItemWriter())
            .build();
    }

    private ItemProcessor<OrderProduct, OrderProduct> settlementProcessor() {
        return orderProduct -> {
            orderProduct.setStatus(OrderStatus.SETTLEMENT);
            return orderProduct;
        };
    }

    @Bean
    public JpaPagingItemReader<OrderProduct> OrderProductItemReader() {

        LocalDate yesterday = LocalDate.now().minusDays(1);

        HashMap<String, Object> param = new HashMap<>();
        param.put("start", yesterday.atStartOfDay());
        param.put("end", yesterday.atStartOfDay().with(LocalTime.MAX));
        param.put("confirm", OrderStatus.CONFIRM);

        return new JpaPagingItemReaderBuilder<OrderProduct>()
            .name("orderProductItemReader")
            .entityManagerFactory(entityManagerFactory)
            .pageSize(chunkSize)
            .queryString("select o from OrderProduct o"
                + " where o.status = :confirm"
                + " and o.confirmedAt between :start and :end"
                + " order by id asc")
            .parameterValues(param)
            .build();
    }

    @Bean
    public JpaItemWriter<OrderProduct> OrderProductItemWriter() {
        JpaItemWriter<OrderProduct> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }

}

package com.batch.batch;

import com.batch.domain.order.OrderProduct;
import com.batch.domain.order.OrderStatus;
import com.batch.domain.seller.Seller;
import com.batch.domain.settlement.DailySettlement;
import com.batch.domain.settlement.DailySettlementRepository;
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
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class SettlementJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final DailySettlementRepository dailySettlementRepository;


    private HashMap<Seller, Long> sellerRevenueMap = new HashMap<>();
    private final int chunkSize = 10;

    @Bean
    public Job SettlementJobBuild() {
        return jobBuilderFactory.get("SettlementJob")
            .start(step1())
            .next(step2())
            .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
            .<OrderProduct, OrderProduct>chunk(chunkSize)
            .reader(orderProductReader())
            .processor(setStatusProcessor())
            .processor(updateRevenueProcessor())
            .writer(orderProductWriter())
            .build();
    }

    @Bean()
    public Step step2() {
        return stepBuilderFactory.get("step2")
            .tasklet((contribution, chunkContext) -> {

                for(Seller seller : sellerRevenueMap.keySet()) {
                    dailySettlementRepository.save(
                        DailySettlement.builder()
                            .seller(seller)
                            .targetDate(LocalDate.now())
                            .amount(sellerRevenueMap.get(seller))
                            .build()
                    );
                }

                return RepeatStatus.FINISHED;
            }).build();
    }

    @Bean
    public JpaPagingItemReader<OrderProduct> orderProductReader() {
        LocalDate yesterday = LocalDate.now().minusDays(1);

        HashMap<String, Object> param = new HashMap<>();
        param.put("start", yesterday.atStartOfDay());
        param.put("end", yesterday.atStartOfDay().with(LocalTime.MAX));
        param.put("confirm", OrderStatus.CONFIRM);

        JpaPagingItemReader<OrderProduct> reader = new JpaPagingItemReader<>() {
            @Override
            public int getPage() {
                return 0;
            }
        };

        reader.setName("orderProductReader");
        reader.setEntityManagerFactory(entityManagerFactory);
        reader.setPageSize(chunkSize);
        reader.setQueryString("select o from OrderProduct o"
            + " where o.status = :confirm"
            + " and o.confirmedAt between :start and :end"
            + " order by id asc");
        reader.setParameterValues(param);

        return reader;
    }

    @Bean
    public ItemProcessor<OrderProduct, OrderProduct> setStatusProcessor() {
        return orderProduct -> {
            orderProduct.setStatus(OrderStatus.SETTLEMENT);
            return orderProduct;
        };
    }

    @Bean
    public ItemProcessor<OrderProduct, OrderProduct> updateRevenueProcessor() {
        return orderProduct -> {

            Seller seller = orderProduct.getProduct().getSeller();

            if(sellerRevenueMap.containsKey(seller)) {
                sellerRevenueMap.put(seller
                    , sellerRevenueMap.get(seller) + Long.valueOf(orderProduct.getAmount()));

                return orderProduct;
            }

            sellerRevenueMap.put(seller, Long.valueOf(orderProduct.getAmount()));
            return orderProduct;
        };
    }

    @Bean
    public JpaItemWriter<OrderProduct> orderProductWriter() {
        JpaItemWriter<OrderProduct> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}

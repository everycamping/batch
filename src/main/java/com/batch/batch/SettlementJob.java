package com.batch.batch;

import com.batch.domain.order.OrderProduct;
import com.batch.domain.order.OrderProductRepository;
import com.batch.domain.order.OrderStatus;
import com.batch.domain.settlement.DailySettlement;
import com.batch.domain.settlement.DailySettlementRepository;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Optional;
import javax.persistence.EntityManager;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class SettlementJob {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final EntityManager entityManager;
    private final DailySettlementRepository dailySettlementRepository;
    private final OrderProductRepository orderProductRepository;


    private final int chunkSize = 10;

    @Bean
    public Job SettlementJobBuild() {
        return jobBuilderFactory.get("SettlementJob")
            .start(SettlementJobStep1())
            .build();
    }

    @Bean
    public Step SettlementJobStep1() {
        return stepBuilderFactory.get("SettlementJob_step1")
            .<OrderProduct, DailySettlement>chunk(chunkSize)
            .reader(Step1Reader())
            .processor(Step1Processor())
            .writer(Step1Writer())
            .build();
    }

    private ItemProcessor<OrderProduct, DailySettlement> Step1Processor() {
        return orderProduct -> {

            DailySettlement dailySettlement = addAmount(orderProduct);
            entityManager.persist(dailySettlement); //영속화
            
            //정산 상태로 변경
            orderProduct.setStatus(OrderStatus.SETTLEMENT);
            orderProduct.setDailySettlement(dailySettlement);
            orderProductRepository.save(orderProduct);

            return dailySettlement;
        };
    }

    private DailySettlement addAmount(OrderProduct orderProduct) {
        Optional<DailySettlement> optionalDailySettlement =
            dailySettlementRepository.findBySellerIdAndTargetDate(
                orderProduct.getProduct().getSeller().getId(),
                LocalDate.now().minusDays(1));

        //update
        if(optionalDailySettlement.isPresent()) {
            DailySettlement dailySettlement = optionalDailySettlement.get();
            dailySettlement.setAmount(dailySettlement.getAmount() + orderProduct.getAmount());
            return dailySettlement;
        }
        //insert
        return DailySettlement.builder()
            .targetDate(LocalDate.now().minusDays(1))
            .seller(orderProduct.getProduct().getSeller())
            .amount(orderProduct.getAmount().longValue())
            .build();
    }

    @Bean
    public JpaPagingItemReader<OrderProduct> Step1Reader() {

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

        reader.setName("step1Reader");
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
    public JpaItemWriter<DailySettlement> Step1Writer() {
        JpaItemWriter<DailySettlement> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);

        return jpaItemWriter;
    }
}

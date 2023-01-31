package com.batch.domain.settlement;

import java.time.LocalDate;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DailySettlementRepository extends JpaRepository<DailySettlement, Long> {
    Optional<DailySettlement> findBySellerIdAndTargetDate(Long sellerId, LocalDate date);
}

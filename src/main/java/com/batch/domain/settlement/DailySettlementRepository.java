package com.batch.domain.settlement;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DailySettlementRepository extends JpaRepository<DailySettlement, Long> {

}

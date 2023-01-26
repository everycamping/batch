package com.batch.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DeptRepository2 extends JpaRepository<Dept2, Long> {

}

package com.study.querydsl.repository;

import com.study.querydsl.entity.Member;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Spring JPA
 */
public interface MemberRepository extends JpaRepository<Member, Long> , MemberRepositoryCustom, QuerydslPredicateExecutor {

    //select m from Member m where m.username = ?
    List<Member> findByUsername(String username);

}

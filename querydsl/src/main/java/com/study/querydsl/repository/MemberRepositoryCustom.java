package com.study.querydsl.repository;

import com.study.querydsl.dto.MemberSearchCondition;
import com.study.querydsl.dto.MemberTeamDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;

/**
 *  Custom Repository ( 복잡한 쿼리 dsl )
 */

public interface MemberRepositoryCustom {
    List<MemberTeamDto> search(MemberSearchCondition condition);

    /**
     * 한번에 
     * Count 쿼리와 content 쿼리 두번 요청됨
     */
    Page<MemberTeamDto> searchPageSimple(MemberSearchCondition condition, Pageable pageable);

    /**
     * 데이터의 내용과 전체 count를 별로도 조회방법
     * 카운트 쿼리랑 content 쿼리 분리해서 별도로
     */
    Page<MemberTeamDto> searchPageComplex(MemberSearchCondition condition, Pageable pageable);
}

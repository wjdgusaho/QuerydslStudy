package com.study.querydsl.repository;

import com.querydsl.jpa.impl.JPAQueryFactory;
import com.study.querydsl.dto.MemberSearchCondition;
import com.study.querydsl.dto.MemberTeamDto;
import com.study.querydsl.entity.Member;
import com.study.querydsl.entity.QMember;
import com.study.querydsl.entity.Team;
import jakarta.persistence.EntityManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
@Slf4j
class MemberRepositoryTest {

    @Autowired
    EntityManager em;

    @Autowired MemberRepository memberRepository;

    //테스트 시작전 세팅 해준다 BeforeEach
    @BeforeEach
    public void before(){
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }


    @Test
    public void basicTest(){
        Member member = new Member("member1", 10);
        memberRepository.save(member);

        Member findMember = memberRepository.findById(member.getId()).get();
        assertThat(findMember).isEqualTo(member);

        List<Member> result1 = memberRepository.findAll();
        assertThat(result1).containsExactly(member);

        List<Member> result2 = memberRepository.findByUsername("member1");
        assertThat(result2).containsExactly(member);
    }


    @Test
    public void searchTest(){

        MemberSearchCondition condition = new MemberSearchCondition();
        //동적 쿼리 데이터가 없으면!! 모든 데이터를 다 가져온다!! ( 동적 쿼리는 limit 나 기본 값이 있으면 좋다 )
        condition.setAgeGoe(35);
        condition.setAgeLoe(40);
        condition.setTeamName("teamB");

        //where 사용
        List<MemberTeamDto> result = memberRepository.search(condition);

        assertThat(result).extracting("username").containsExactly("member4");
    }

    @Test
    public void searchPageSimpleTest(){

        MemberSearchCondition condition = new MemberSearchCondition();
        PageRequest pageRequest= PageRequest.of(0, 3);  //0페이지 3개

        //where 사용
        Page<MemberTeamDto> result = memberRepository.searchPageSimple(condition, pageRequest);

        assertThat(result.getSize()).isEqualTo(3);
        assertThat(result.getContent()).extracting("username").containsExactly("member1", "member2", "member3");
    }


    /**
     * 한계점
     * 조인 X ( 묵시적 조인은 가능하나 left join 불가능 )
     * 클라이언트 Querydsl에 의존해야한다. 서비스 클래스가 Querydsl이라는 구현 기술에 의존해야한다.
     * 복잡한 실무환경에서 사용하기에는 한계가 명확하다.
     */
    @Test
    public void querydslPredicationexecutorTest(){
        QMember member = QMember.member;
        Iterable<Member> result = memberRepository.findAll(
                member.age.between(20, 40)
                        .and(member.username.eq("member1"))
        );
        for(Member findMember : result){
           // System.out.println("member1 = " + findMember);
            log.info("member1 = " + findMember);
        }
    }
}
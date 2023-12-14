package com.study.querydsl.repository;

import com.querydsl.core.QueryResults;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.JPQLQuery;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.study.querydsl.dto.MemberSearchCondition;
import com.study.querydsl.dto.MemberTeamDto;
import com.study.querydsl.dto.QMemberTeamDto;
import com.study.querydsl.entity.Member;
import jakarta.persistence.EntityManager;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.support.QuerydslRepositorySupport;
import org.springframework.data.support.PageableExecutionUtils;

import java.util.List;

import static com.study.querydsl.entity.QMember.member;
import static com.study.querydsl.entity.QTeam.team;
import static org.springframework.util.StringUtils.hasText;

/**
 *  Repository + Impl (규칙)
 */

/**
 *  extends QuerydslRepositorySupport
 *  장점
 *   getQuerydsl().applyPagination() 스프링 데이터가 제공하는 페이지 쿼리DSL로 편리하게 변환 가능 ( 단 Srot 오류발생 )
 *   from() 절로 시작 가능 ( 최근 QueryFactory를 사용해서 Select() 로 시작하는 것이 명시적 )
 *   EntitiyManager 제공
 *
 *   단점
 *   Querydsl 3.x 버전을 대상으로 만들어짐
 *   Queryddl 4.x 에서 나온 JPAQueryFactory로 시작할수 없음
 *  스피링 데이터 Sort 기능이 정상 동작하지 않음
 *
 */
public class MemberRepositoryImpl  implements MemberRepositoryCustom{

//    //QuerydslRepositorySupport 를 사용하면 필요없다
    private final JPAQueryFactory queryFactory;

    public MemberRepositoryImpl(EntityManager em){
        this.queryFactory = new JPAQueryFactory(em);
    }

    /*
    //QuerydslRepositorySupport
    public MemberRepositoryImpl(){
        //부모 QuerydslRepositorySupport 에서 ( JPAQueryFactory 가 선언되어있음 )
        super(Member.class);
    }
    */

    @Override
    public List<MemberTeamDto> search(MemberSearchCondition condition) {

        /*
        //QuerydslRepositorySupport
        List<MemberTeamDto> QuerydslRepositorySupportResult = from(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .select(new QMemberTeamDto(
                        member.id.as("memberId"),
                        member.username,
                        member.age,
                        team.id.as("teamId"),
                        team.name.as("teamName")))
                .fetch();
        */
        return queryFactory
                .select(new QMemberTeamDto(
                        member.id.as("memberId"),
                        member.username,
                        member.age,
                        team.id.as("teamId"),
                        team.name.as("teamName")
                ))
                .from(member)
                .leftJoin(member.team, team)
                .where(
                        //동적 쿼리
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .fetch();
    }

    @Override
    public Page<MemberTeamDto> searchPageSimple(MemberSearchCondition condition, Pageable pageable) {
        QueryResults<MemberTeamDto> results = queryFactory
                .select(new QMemberTeamDto(
                        member.id.as("memberId"),
                        member.username,
                        member.age,
                        team.id.as("teamId"),
                        team.name.as("teamName")
                ))
                .from(member)
                .leftJoin(member.team, team)
                .where(
                        //동적 쿼리
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .offset(pageable.getOffset())
                .limit(pageable.getPageSize())
                .fetchResults();

        List<MemberTeamDto> content = results.getResults();
        long total = results.getTotal();

        //PageImpl = Spring Data의 Page 구현체
        return new PageImpl<>(content, pageable, total);

    }

    /*
    // QuerydslRepositorySupport
    public Page<MemberTeamDto> searchPageSimple2(MemberSearchCondition condition, Pageable pageable) {

        JPQLQuery<MemberTeamDto> jpaQuery = from(member)
                .leftJoin(member.team, team)
                .where(
                        //동적 쿼리
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .select(new QMemberTeamDto(
                        member.id.as("memberId"),
                        member.username,
                        member.age,
                        team.id.as("teamId"),
                        team.name.as("teamName")
                ));

        JPQLQuery<MemberTeamDto> query = getQuerydsl().applyPagination(pageable, jpaQuery);

        List<MemberTeamDto> fetch = query.fetch();

        List<MemberTeamDto> content = results.getResults();
        long total = results.getTotal();

        //PageImpl = Spring Data의 Page 구현체
        return new PageImpl<>(content, pageable, total);

    }
    */

    @Override
    public Page<MemberTeamDto> searchPageComplex(MemberSearchCondition condition, Pageable pageable) {

        List<MemberTeamDto> content = queryFactory
                .select(new QMemberTeamDto(
                        member.id.as("memberId"),
                        member.username,
                        member.age,
                        team.id.as("teamId"),
                        team.name.as("teamName")
                ))
                .from(member)
                .leftJoin(member.team, team)
                .where(
                        //동적 쿼리
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                )
                .offset(pageable.getOffset())
                .limit(pageable.getPageSize())
                .fetch();

        // 카운트 쿼리를 최적화 하기 위해서! ( 효율화! )  - 데이터가 많다면!
        // 특정 상황에서는 Join을 할 필요가 없을 수도 있기 때문!
        //featch 나 featchCount를 해줘야 쿼리를 요청하는데 없으면 실제로 요청하진 않는다
        JPAQuery<Member> countQuery = queryFactory
                .selectFrom(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                );
        
        //getPage에서 content와 pageable의 totalSize를 보고 page의 시작이면서 contentSize보다 작거나 마지막 페이지면
        //함수를 호출안한다 ( () -> contQuery.fetchCount() )
        return PageableExecutionUtils.getPage(content, pageable, countQuery::fetchCount);  //() -> countQuery.fetchCount()
        //return new PageImpl<>(content, pageable, total);
    }

    private BooleanExpression usernameEq(String username) {
        return hasText(username) ? member.username.eq(username) : null;
    }

    private BooleanExpression teamNameEq(String teamName) {
        return hasText(teamName) ? team.name.eq(teamName) : null;
    }

    private BooleanExpression ageGoe(Integer ageGoe) {
        return ageGoe != null ? member.age.goe(ageGoe) : null;
    }

    private BooleanExpression ageLoe(Integer ageLoe) {
        return ageLoe != null ? member.age.loe(ageLoe) : null;
    }

}

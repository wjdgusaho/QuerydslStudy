package com.study.querydsl.repository;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.study.querydsl.dto.MemberSearchCondition;
import com.study.querydsl.dto.MemberTeamDto;
import com.study.querydsl.dto.QMemberDto;
import com.study.querydsl.dto.QMemberTeamDto;
import com.study.querydsl.entity.Member;
import com.study.querydsl.entity.QMember;
import jakarta.persistence.EntityManager;
import org.springframework.stereotype.Repository;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Optional;

import static com.study.querydsl.entity.QMember.*;
import static com.study.querydsl.entity.QTeam.team;
import static org.springframework.util.StringUtils.hasText;
import static org.springframework.util.StringUtils.isEmpty;

//순수 JPA 레파지 토리
@Repository
public class MemberJpaRepository {

    //싱글톤 (동시성 문제 안생김!! - Spring에선 EntityManager 트랙션 바인딩만 해준다..? )
    private final EntityManager em;
    private final JPAQueryFactory queryFactory;

    public MemberJpaRepository(EntityManager em){
        this.em = em;
        this.queryFactory = new JPAQueryFactory(em);
    }

    /*
    //Bean 등록했다면 ( new JPAQueryFactory(em) ) 단 아래의 경우 Test에서 따로 설정해줘야하는게 생김! 주입받는게 2개 이기 때문
    public MemberJpaRepository(EntityManager em, JPAQueryFactory queryFactory){
        this.em = em;
        this.queryFactory = queryFactory;  //Bean 설정했기때문! (이렇게 해야 lombok의 @RequiredArgsConstructor 를 해주면 생략가능!)
    }
    */

    public void save(Member member){
        em.persist(member);
    }

    public Optional<Member> findById(Long id){
        Member findMember = em.find(Member.class, id);
        return Optional.ofNullable(findMember);
    }

    public List<Member> findAll(){
        return em.createQuery("select m from Member m", Member.class)
                .getResultList();
    }

    public List<Member> findAll_Querydsl(){
        return queryFactory
                .selectFrom(member)
                .fetch();
    }

    public List<Member> findByUsername(String username){
        return em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", username)
                .getResultList();
    }

    public List<Member> findByUsername_Querydsl(String username){
        return queryFactory
                .selectFrom(member)
                .where(member.username.eq(username))
                .fetch();
    }

    public List<MemberTeamDto> searchBtBuilder(MemberSearchCondition condition){

        BooleanBuilder builder = new BooleanBuilder();
        //StringUtils.hasText 는 null or ""
        if(hasText(condition.getUsername())){
            builder.and(member.username.eq(condition.getUsername()));
        }
        if(hasText(condition.getTeamName())){
            builder.and(team.name.eq(condition.getTeamName()));
        }
        if(condition.getAgeGoe() != null){
            builder.and(member.age.goe(condition.getAgeGoe()));
        }
        if(condition.getAgeLoe() != null){
            builder.and(member.age.loe(condition.getAgeLoe()));
        }

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
                .where(builder)
                .fetch();
    }

    public List<MemberTeamDto> search(MemberSearchCondition condition){
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

    //predicate 말고 BooleanExpression 경우 조합이 가능하다 (위 메소드처럼 )
    private BooleanExpression ageBetween(int ageLoe, int ageGoe){
        return ageLoe(ageLoe).and(ageGoe(ageGoe));
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

package com.study.querydsl.repository;

import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQuery;
import com.study.querydsl.dto.MemberSearchCondition;
import com.study.querydsl.entity.Member;
import com.study.querydsl.entity.QMember;
import com.study.querydsl.repository.support.Querydsl4RepositorySupport;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.support.PageableExecutionUtils;
import org.springframework.stereotype.Repository;

import java.util.List;

import static com.study.querydsl.entity.QMember.member;
import static com.study.querydsl.entity.QTeam.team;
import static org.springframework.util.StringUtils.hasText;

@Repository
public class MemberTestRepository extends Querydsl4RepositorySupport {

    public MemberTestRepository(){
        super(Member.class);
    }

    public List<Member> basicSelect(){
        return select(member)
                .from(member)
                .fetch();
    }

    public List<Member> basicSelectFrom(){
        return selectFrom(member)
                .fetch();
    }

    /**
     * QuerydslRepositorySupport 로 할경우 
     * 현재는 Querydsl4RepositorySupport 이기때문에 sort 등 다 가능
     */
    public Page<Member> searchPageByApplyPage(MemberSearchCondition condition, Pageable pageable){
        JPAQuery<Member> query = selectFrom(member)
                .leftJoin(member.team, team)
                .where(
                        usernameEq(condition.getUsername()),
                        teamNameEq(condition.getTeamName()),
                        ageGoe(condition.getAgeGoe()),
                        ageLoe(condition.getAgeLoe())
                );

        //sort 동적으로 하려면 getQuerydsl().applyPagination(pageable, query) 이렇게 해야한다
        List<Member> content = getQuerydsl().applyPagination(pageable, query)
                .fetch();
        
        return PageableExecutionUtils.getPage(content, pageable, query::fetchCount);
    }
    
    
    // 위 searchPageByApplyPage 와 동일함
    public Page<Member> applyPagination(MemberSearchCondition condition, Pageable pageable){
        //Querydsl4RepositorySupport 에서 만든 함수
        //applyPagination 안에
        //  List<Member> content = getQuerydsl().applyPagination(pageable, query).fetch();
        //  return PageableExecutionUtils.getPage(content, pageable, query::fetchCount);
        //가 존재
        return applyPagination(pageable, query ->
                query.selectFrom(member)
                        .leftJoin(member.team, team)
                        .where(
                                usernameEq(condition.getUsername()),
                                teamNameEq(condition.getTeamName()),
                                ageGoe(condition.getAgeGoe()),
                                ageLoe(condition.getAgeLoe())
                        )
        );
    }


    public Page<Member> applyPagination2(MemberSearchCondition condition, Pageable pageable){
        return applyPagination(pageable,
                contentQuery -> contentQuery
                        .selectFrom(member)
                        .leftJoin(member.team, team)
                        .where(
                                usernameEq(condition.getUsername()),
                                teamNameEq(condition.getTeamName()),
                                ageGoe(condition.getAgeGoe()),
                                ageLoe(condition.getAgeLoe())
                        ),
                countQuery -> countQuery
                        .select(member.id)
                        .from(member)
                        .where(
                                usernameEq(condition.getUsername()),
                                teamNameEq(condition.getTeamName()),
                                ageGoe(condition.getAgeGoe()),
                                ageLoe(condition.getAgeLoe())
                        )
        );
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

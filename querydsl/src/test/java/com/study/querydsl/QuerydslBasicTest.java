package com.study.querydsl;


import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.study.querydsl.dto.MemberDto;
import com.study.querydsl.dto.QMemberDto;
import com.study.querydsl.dto.UserDto;
import com.study.querydsl.entity.Member;
import com.study.querydsl.entity.QMember;
import com.study.querydsl.entity.QTeam;
import com.study.querydsl.entity.Team;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static com.study.querydsl.entity.QMember.*;
import static com.study.querydsl.entity.QTeam.team;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

    //테스트 시작전 세팅 해준다 BeforeEach
    @BeforeEach
    public void before(){
        queryFactory = new JPAQueryFactory(em);
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


    //사용자가 실행해서 메소드를 실행했을때 오류를 알수있다. ( 런타임 오류 )
    @Test
    public void startJPQL(){
        //member1을 찾아라.
        String qlString = "select m from Member m "+
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")  //파라미터 바인딩
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }


    //QueryDSL
    //Q 타입을 생성하는 이유는 실행하면 컴파일 오류가 발생한다! (즉 문법 오류를 틀리수가 없다! )
    //파라미터 바인딩을 자동으로 해준다!
    @Test
    public void startQuerydsl(){
        //필드로 가져가도된다!
        //JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        QMember m = new QMember("m");

        Member findMember = queryFactory
                .select(m)
                .from(m)
                .where(m.username.eq("member1"))
                .fetchOne();    //단 건 조회

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    //코드 줄이기 <- 권장
    //QMember.member 를 Static 으로 선언
    //쿼리DSL은 JPQL 빌더 역할을 한다 ( 즉 쿼리DSL은 결과적으로 JPQL로 작성이된다고 생각하면됨)
    @Test
    public void startQuerydsl2(){
        //같은 테이블을 조인해야하는경우에만 이런식으로 사용 아래도 member -> m1으로 변경
        //QMember m1 = new QMember("m1");

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1")) //파라미터 바인딩 처리
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }


    @Test
    public void search(){
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");

    }

    //위와 똑같은 AND 연산 ( where 에서 .and 대신 ", " 할경우 And 와 같은 역할을 한다 )
    @Test
    public void searchAndParam(){
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        (member.age.eq(10))
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");

    }

    @Test
    public void resultFetch(){
//        List<Member> fetch = queryFactory
//                .selectFrom(member)
//                .fetch();  //결과를 리스트로(여러건) 조회
//
//        Member fetchOne = queryFactory
//                .selectFrom(member)
//                .fetchOne();
//
//        Member fetchFirst = queryFactory
//                .selectFrom(member)
//                .fetchFirst();

        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();

        long total = queryFactory.selectFrom(member).fetchCount();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단 2에서 회원 이름이 없드면 마지만ㄱ에 출력 (nulls last)
     */
    @Test
    public void sort(){
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        //result의 결과값을 하나씩 넣어줌
        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        //검증 ( 이게 이거 맞아 ? 확인용 )
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    public void paging1(){
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2(){
        // 실무에서는 사용 못할수도 있다 ( count 쿼리를 분리해야한다 )
        // content 쿼리는 복잡한데 count 쿼리가 단순하게 할 수 있다면!!
        // 두개를 따로 하는게 좋다! ( 성능상 좋다 )
        //먼저 count 쿼리 이후에 content 쿼리로 요청 2번한다
        QueryResults<Member> queryResult = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(queryResult.getTotal()).isEqualTo(4);
        assertThat(queryResult.getLimit()).isEqualTo(2);
        assertThat(queryResult.getOffset()).isEqualTo(1);
        assertThat(queryResult.getResults().size()).isEqualTo(2);

    }

    @Test
    public void aggregation(){
        //querydsl Tuple - 여러개의 타입이 있을때
        //실무에선 Tuple 보단 DTO를 사용한다!
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구하라
     */
    @Test
    public void group() throws Exception{
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)  //member.team 과 team 을 조인한다
                .groupBy(team.name)       //team의 name으로 그룹화 한다
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15); // (10 + 20) / 2

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35); // (30 + 40) / 2
    }

    @Test
    public void join(){
        List<Member> result = queryFactory
                .selectFrom(member)
                //.join(member.team, team)
                .leftJoin(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조인
     */
    @Test
    public void theta_join(){
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        //left outjoin(외부조인)사용 불가능
        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }


    /**
     *  회원과 팀을 조인하면서, 팀 이림이 teamA인 팀만 조인 , 회원은 모두 조회
     *  JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering(){
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        for(Tuple tuple : result){
            System.out.println("tuple = " + tuple);
        }

    }

    /**
     * 연관관계 없는 엔티티 외부 조긴
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     */
    @Test
    public void join_on_no_relation(){
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for(Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 페치 조인 : 기존 SQL 제공하는 기능 X
     * SQL 조인을 활용해 연관된 엔티티를 SQL 한번에 조회하는 기능
     * 주로 성능 최적화에 사용하는 방법
     */
    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo(){
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("패치 조인 미적용").isFalse();
    }

    @Test
    public void fechJoinUse(){
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("패치 조인 적용").isTrue();
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions.select(
                                        memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryGoe(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions.select(
                                        memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryIn(){

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }


    @Test
    public void selectSubQuery(){
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub))
                .from(member)
                .fetch();

        for(Tuple tuple : result){
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * JPA JPQL 서브쿼리의 한계점
     * - from 절의 서브쿼리(인라인 뷰)는 지원하지 않는다
     * - Querydsl도 지원하지 X
     *
     * 해결 방안
     * 1. 서브쿼리를 join으로 변경 ( 가능한 사항도 있고, 불가능한 사항도 있다)
     * 2. 애플리케이션에서 쿼리를 2번 분리해서 실행한다
     * 3. nativeSQL을 사용한다.
     *
     * SQL 에선 데이터 가져오는 것에 집중하자 (필터링 등으로 데이터를 최소화 하는 느낌 )
     * 한방 쿼리면 다 잘되냐..?
     * 차라리 쿼리를 두번 세번 나눠서 하는게 좋을 수도 있다
     * sql AntiPatterns - 개발자가 알아야 할 25가지 SQL 함정과 해법
     * -> 복잡한 쿼리를 쪼개서 하면 분량을 줄일수도 있을거다!!
     *
     */

    /**
     * case문
     */
    @Test
    public void basicCase(){
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for(String s : result){
            System.out.println("s = " + s);
        }
    }

    @Test
    public void complexCase(){
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for(String s : result){
            System.out.println("s = " + s);
        }
    }

    /**
     * 상수 , 문자 더하기
     */
    @Test
    public void constant(){
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for(Tuple tuple: result){
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    public void concat(){
        //{username}_{age}
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for(String s : result){
            System.out.println("s = " + s);
        }

    }


    @Test
    public void simpleProjection(){
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        for(String s : result){
            System.out.println("s = " + s);
        }
    }

    @Test
    public void tupleProjection(){
        List<Tuple> result = queryFactory
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for(Tuple tuple : result){
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    @Test
    public void findDtoByJPQL(){
        //new 오퍼레이션 문법
        List<MemberDto> result = em.createQuery("select new com.study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        for(MemberDto memberDto : result){
            System.out.println("memberDto : " + memberDto);
        }
    }


    @Test
    public void findDtoBySetter(){
        //bean 은 getter 와 setter 로 값을 넣는다
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for(MemberDto memberDto : result){
            System.out.println("memberDto : " + memberDto);
        }
    }

    @Test
    public void findDtoByField(){
        //setter없이 필드에 값을 바로 넣는다
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for(MemberDto memberDto : result){
            System.out.println("memberDto : " + memberDto);
        }
    }

    @Test
    public void findDtoByConstructor(){
        //생성자를 호출하여 생성한다
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for(MemberDto memberDto : result){
            System.out.println("memberDto : " + memberDto);
        }
    }

    /**
     * 이름이 다를때와 서브쿼리 사용할경우
     */
    @Test
    public void findUserDto(){
        QMember memberSub = new QMember("memberSub");
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),  //이름이 다를떄는 별칭 사용 가능!!
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")    //서브쿼리 결과에 별칭!
                ))
                .from(member)
                .fetch();

        for(UserDto userDto : result){
            System.out.println("userDto : " + userDto);
        }
    }


    @Test
    public void findDtoByConstructorUserDto(){
        //생성자를 호출하여 생성한다 ( 순서에 맞추면 되기 때문에 별칭 필요 없다 )
        //코드가 실행하는 순간에서야 문제를 찾을 수 있다 ( RunTime 오류 )
        List<UserDto> result = queryFactory
                .select(Projections.constructor(UserDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for(UserDto userDto : result){
            System.out.println("userDto : " + userDto);
        }
    }

    @Test
    public void findDtoByQueryProjection(){
        // QueryProjection의 경우 Q 파일 생성하기 때문에! ( 컴파일 오류! )
        // 단점이 Q파일을 생성해야한다
        // MemberDto 자체가 Querydsl의 의존성이 추가된다!
        // MemberDto 가 Service Repository Cotroller 에서도 쓰이는데 Querydsl 의존성이 추가되기 때문에 아키텍쳐의 설계 고민..
        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        for(MemberDto memberDto : result){
            System.out.println("memberDto " + memberDto);
        }

    }

    /**
     * 동적 쿼리 ( BooleanBuilder )
     */
    @Test
    public void dynamicQuery_BooleanBuilder(){
        String usernameParam = "member1";
        Integer ageParam = 10;

        List <Member> result = searchMember1(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {
        //memberusername 이 필수다!! 아래와 같이 초기값을 줄 수 있다.
        //BooleanBuilder builder = new BooleanBuilder(member.username.eq(usernameCond));
        BooleanBuilder builder = new BooleanBuilder();

        if(usernameCond != null){
            builder.and(member.username.eq(usernameCond));
        }
        if(ageCond != null){
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();

    }

    /**
     * 동적 쿼리 ( Where문 다중 파라미터 )
     */
    @Test
    public void dynamicQuery_WhereParam(){
        String usernameParam = "member1";
        Integer ageParam = 10;

        List <Member> result = searchMember2(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    //아래와 같은 방법은
    //Wher 조건에 null 값은 무시된다 ( null 체크는 주의해서 사용해야한다!! )
    //메서드 다른 쿼리에서도 재활용 가능
    //쿼리 자체의 가독성을 높인다.

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(member)
                //.where(usernameEq(usernameCond), ageEq(ageCond))
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private Predicate allEq(String ussernameCond, Integer ageCond){
        //null 처리 해줘야하긴하다!
        return usernameEq(ussernameCond).and(ageEq(ageCond));
    }

    //private Predicate ageEq(Integer ageCond){
    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond == null ? null : member.age.eq(ageCond);
    }

    private BooleanExpression usernameEq(String usernameCond) {
        if(usernameCond == null) return null;
        return member.username.eq(usernameCond);
    }


    /**
     * 수정, 삭제 배치 쿼리
     * 쿼리 한번에 대량 데이터 수정! ( 벌크 연산 )
     * 변경 감지.. 한쿼리로 처리하면
     */
    @Test
    public void bulkUpdate(){

        //member1 = 10 -> DB member1
        //member2 = 20 -> DB member2
        //member3 = 30 -> DB member3
        //member4 = 40 -> DB member4

        //Jpa는 영속성 컨텍스트에 Entity가 올라가있는다
        //현재 member 1 2 3 4 가 올라가 있는 상태이다.
        //벌크 연산은 영속성 컨텍스트를 무시하고 DB에 바로 실행
        //즉. DB와 영속성 컨텍스트와 값이 달라진다!

        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        //1 member1 = 10 -> 1 DB 비회원
        //2 member2 = 20 -> 2 DB 비회원
        //3 member3 = 30 -> 3 DB member3
        //4 member4 = 40 -> 4 DB member4
        
        //Select 해서 DB에서 가져와도! 이미 영속성 컨텍스트에 값이 있으면
        //영속성 컨텍스트가 우선 순위로 영속성 컨텍스 안에 있는 값을 출력 한다!
        //( DB에서 가져온값을 버리고 영속성 컨텍스 값을 가져옴 )

        //따라서.  벌크 연산을 했다면! -> 영속성 컨텍스를 날려라!
        em.flush();
        em.clear();

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        for(Member member1 : result){
            System.out.println("member1 = " + member1);
        }
    }

    /**
     * 연산 - 벌크연산 ( 더하기 , 곱하기 )
     */
    @Test
    public void blukAdd(){
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.add(1))   //곱하기 .multiply(2)
                .execute();

    }

    /**
     * 삭제 - 벌크 연산
     */
    @Test
    public void bulkDelete(){
        long count = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    /**
     * SQL function 호출
     */
    @Test
    public void sqlFunction(){
        // H2를 사용하기 때문에 function 을 사용하려면
        // H2 Direct 에 registeFunction 에 등록되어 있어야한다!
        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username, "member", "M"))
                .from(member)
                .fetch();

        for(String s : result){
            System.out.println("s = " + s);
        }
    }

    @Test
    public void sqlFunction2(){
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate("function('lower', {0})", member.username)))
                .where(member.username.eq(member.username.lower()))
                .fetch();

        for (String s: result){
            System.out.println("s = " + s);
        }
    }

}


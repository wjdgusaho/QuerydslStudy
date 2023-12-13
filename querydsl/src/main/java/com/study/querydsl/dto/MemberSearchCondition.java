package com.study.querydsl.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter @Setter
public class MemberSearchCondition {
    //회원 명 , 팀명, 나이 (agGoe, ageLoe)
    private String username;
    private String teamName;
    private Integer ageGoe;
    private Integer ageLoe;

}

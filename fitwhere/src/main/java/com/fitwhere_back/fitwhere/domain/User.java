package com.fitwhere_back.fitwhere.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Getter
@Setter
@NoArgsConstructor
public class User extends BaseTimeEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String email;
    private String password;
    private String name;
    private Integer age;
    private String gender;
    private String region;
    private String interestSport;
    private String purpose;
    private Boolean locationAgreed;
    // SNS 로그인, 프로필 등 추가 필드 필요시 확장
}

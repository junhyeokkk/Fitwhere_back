# Fitwhere_back Spring Boot 백엔드 환경 세팅 문서

## 1. 프로젝트 구조
- Gradle, Java 21 기반 Spring Boot 프로젝트
- 주요 패키지 구조:
  - `controller`: REST API 컨트롤러
  - `service`: 비즈니스 로직
  - `repository`: JPA 데이터 접근
  - `domain`: 엔티티 클래스
  - `common`: 공통 응답, 예외처리, 유틸리티

## 2. 공통 API 응답 구조
- `ApiResponse<T>`: 모든 API 응답을 일관되게 감싸는 제네릭 클래스
- `ApiError`: 에러 발생 시 응답에 포함되는 에러 정보(코드, 메시지)
- `ApiResponseUtil`: 성공/실패 응답을 쉽게 생성하는 유틸리티

### 예시
- 성공: `return ApiResponseUtil.ok(data);`
- 실패: `return ApiResponseUtil.fail("ERROR_CODE", "에러 메시지");`

## 3. 공통 예외 처리
- `GlobalExceptionHandler`: 모든 컨트롤러에서 발생하는 예외를 한 곳에서 처리
- 예외 발생 시 ApiError로 변환하여 일관된 에러 응답 반환

## 4. DB 및 환경설정
- `src/main/resources/application.yml`에서 DB 연결, JPA, 서버 포트 등 설정
- 예시:
  ```yml
  spring:
    datasource:
      url: jdbc:mysql://localhost:3306/fitwhere?serverTimezone=Asia/Seoul&characterEncoding=UTF-8
      username: root
      password: your_password
      driver-class-name: com.mysql.cj.jdbc.Driver
    jpa:
      hibernate:
        ddl-auto: update
      show-sql: true
      properties:
        hibernate:
          format_sql: true
    profiles:
      active: dev
  server:
    port: 8080
  ```

## 5. Git 관리
- `.gitignore`에 빌드, IDE, OS, 로그 등 불필요 파일/폴더 제외

## 6. Swagger 등 API 문서화
- `SwaggerConfig`로 API 문서 자동화 (설정 파일 참고)

## 7. 개발/운영 환경 분리
- `application-dev.yml`, `application-prod.yml` 등으로 profile 분리 가능

## 8. 기타
- Entity, Repository, Service, Controller 기본 구조 설계
- 테스트 코드(JUnit 등) 작성 환경 필요시 추가

---

### 문의 및 추가 세팅 필요시 담당자에게 문의 바랍니다.


package com.fitwhere_back.fitwhere.controller;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import com.fitwhere_back.fitwhere.common.ApiResponse;
import com.fitwhere_back.fitwhere.common.ApiResponseUtil;

@RestController
@RequestMapping("/api/facilities")
public class FacilityController {

    /**
     * 시설 목록 조회 예시 (성공 응답)
     * 실제 서비스에서는 service에서 데이터를 받아와 data에 넣어줍니다.
     */
    @GetMapping("")
    @ResponseBody
    public ApiResponse<String> getFacilities() {
        // 예시 데이터 (실제에선 List<Facility> 등으로 대체)
        String data = "시설 목록 예시";
        // 성공 응답: data만 전달
        return ApiResponseUtil.ok(data);
    }

    /**
     * 에러 응답 예시
     */
    @GetMapping("/error-example")
    @ResponseBody
    public ApiResponse<Void> errorExample() {
        // 실패 응답: 에러코드와 메시지 전달
        return ApiResponseUtil.fail("FACILITY_NOT_FOUND", "시설을 찾을 수 없습니다.");
    }
}

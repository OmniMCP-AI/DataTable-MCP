#!/bin/bash

# ============================================
# Jenkins 构建触发脚本
# 用途: 通过 curl 调用 Jenkins API 触发 excelize-mcp 项目构建
# ============================================

set -e

# ===== 配置参数 =====
JENKINS_URL="https://jenkins.footprint.cool"
# JOB_PATH 默认值,可通过 -j 参数覆盖
DEFAULT_JOB_PATH="view/omcp/job/excelize-mcp"

# ===== 帮助信息 =====
show_help() {
    cat << EOF
使用方法: $0 [选项]

描述:
    通过 Jenkins API 触发 Jenkins 项目构建

选项:
    -u, --user <用户名>         Jenkins 用户名 (必填)
    -t, --token <API Token>     Jenkins API Token (必填)
    -j, --job <任务路径>        Jenkins Job 路径 (默认: view/omcp/job/excelize-mcp)
    -e, --env <环境>            部署环境 (默认: prod)
    -b, --branch <分支名>       Git 分支名 (默认: main)
    -w, --wait                  等待构建完成
    -h, --help                  显示此帮助信息

示例:
    # 基本使用 (使用默认 job)
    $0 -u admin -t your_api_token

    # 指定 job 名称
    $0 -u admin -t your_api_token -j view/omcp/job/my-service

    # 指定环境和分支
    $0 -u admin -t your_api_token -j view/omcp/job/excelize-mcp -e prod -b main

    # 等待构建完成
    $0 -u admin -t your_api_token -j view/omcp/job/excelize-mcp -w

注意:
    1. API Token 可以在 Jenkins 用户配置中生成
    2. 使用 -w 选项会持续查询构建状态直到完成
EOF
}

# ===== 参数解析 =====
JENKINS_USER=""
JENKINS_TOKEN=""
JOB_PATH="$DEFAULT_JOB_PATH"
ENVIRONMENT="prod"
BRANCH_NAME="main"
WAIT_FOR_COMPLETION=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--user)
            JENKINS_USER="$2"
            shift 2
            ;;
        -t|--token)
            JENKINS_TOKEN="$2"
            shift 2
            ;;
        -j|--job)
            JOB_PATH="$2"
            shift 2
            ;;
        -e|--env)
            ENVIRONMENT="$2"
            shift 2
            ;;
        -b|--branch)
            BRANCH_NAME="$2"
            shift 2
            ;;
        -w|--wait)
            WAIT_FOR_COMPLETION=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "错误: 未知参数 '$1'"
            show_help
            exit 1
            ;;
    esac
done

# ===== 参数验证 =====
if [ -z "$JENKINS_USER" ]; then
    echo "错误: 必须提供 Jenkins 用户名 (-u)"
    show_help
    exit 1
fi

if [ -z "$JENKINS_TOKEN" ]; then
    echo "错误: 必须提供 Jenkins API Token (-t)"
    show_help
    exit 1
fi

# ===== 触发构建 =====
echo "========================================"
echo "触发 Jenkins 构建"
echo "========================================"
echo "Jenkins URL: $JENKINS_URL"
echo "Job Path: $JOB_PATH"
echo "环境: $ENVIRONMENT"
echo "分支: $BRANCH_NAME"
echo "========================================"

# 构建 API URL
BUILD_URL="${JENKINS_URL}/${JOB_PATH}/buildWithParameters"

# 发送构建请求
echo "正在发送构建请求..."
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    --user "${JENKINS_USER}:${JENKINS_TOKEN}" \
    --data-urlencode "ENVIRONMENT=${ENVIRONMENT}" \
    --data-urlencode "BRANCH_NAME=${BRANCH_NAME}" \
    "${BUILD_URL}")

# 解析响应
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" -eq 201 ]; then
    echo "✓ 构建已成功触发!"

    # 从响应头获取队列 URL
    QUEUE_URL=$(echo "$BODY" | grep -i "Location:" | awk '{print $2}' | tr -d '\r')

    if [ -n "$QUEUE_URL" ]; then
        echo "队列 URL: $QUEUE_URL"
    fi

    # 如果需要等待构建完成
    if [ "$WAIT_FOR_COMPLETION" = true ]; then
        echo ""
        echo "等待构建开始..."
        sleep 5

        # 获取最新构建编号
        LAST_BUILD_URL="${JENKINS_URL}/${JOB_PATH}/lastBuild/api/json"
        BUILD_INFO=$(curl -s --user "${JENKINS_USER}:${JENKINS_TOKEN}" "${LAST_BUILD_URL}")
        BUILD_NUMBER=$(echo "$BUILD_INFO" | grep -o '"number":[0-9]*' | head -1 | cut -d':' -f2)

        if [ -n "$BUILD_NUMBER" ]; then
            echo "构建编号: #${BUILD_NUMBER}"
            BUILD_STATUS_URL="${JENKINS_URL}/${JOB_PATH}/${BUILD_NUMBER}/api/json"

            # 轮询构建状态
            while true; do
                STATUS_INFO=$(curl -s --user "${JENKINS_USER}:${JENKINS_TOKEN}" "${BUILD_STATUS_URL}")
                BUILDING=$(echo "$STATUS_INFO" | grep -o '"building":[^,]*' | cut -d':' -f2)
                RESULT=$(echo "$STATUS_INFO" | grep -o '"result":"[^"]*"' | cut -d'"' -f4)

                if [ "$BUILDING" = "false" ]; then
                    echo ""
                    echo "========================================"
                    echo "构建完成!"
                    echo "结果: $RESULT"
                    echo "查看详情: ${JENKINS_URL}/${JOB_PATH}/${BUILD_NUMBER}/"
                    echo "========================================"

                    if [ "$RESULT" = "SUCCESS" ]; then
                        exit 0
                    else
                        exit 1
                    fi
                else
                    echo -n "."
                    sleep 5
                fi
            done
        else
            echo "警告: 无法获取构建编号"
        fi
    else
        echo ""
        echo "查看构建状态: ${JENKINS_URL}/${JOB_PATH}"
    fi

elif [ "$HTTP_CODE" -eq 401 ]; then
    echo "✗ 认证失败! 请检查用户名和 API Token"
    exit 1
elif [ "$HTTP_CODE" -eq 403 ]; then
    echo "✗ 权限不足! 用户 '$JENKINS_USER' 没有触发构建的权限"
    exit 1
elif [ "$HTTP_CODE" -eq 404 ]; then
    echo "✗ 找不到任务! 请检查 Jenkins URL 和 Job Path"
    exit 1
else
    echo "✗ 构建触发失败!"
    echo "HTTP 状态码: $HTTP_CODE"
    echo "响应内容: $BODY"
    exit 1
fi

import time

import googleapiclient.discovery
import googleapiclient.errors

PROJECT_ID = "long-centaur-402106"
ZONE = "us-central1-a"  # VM을 생성할 GCP Zone
VM_MACHINE_TYPE = "n1-standard-4"
VM_NAME = "FullPipeline-vm"
# # 실제 MLOps 코드가 담긴 GCS 버킷 및 파일 경로
GCS_BUCKET = "snp-project-bucket"
# MLOPS_SCRIPT_PATH = "run_mlops.sh"


def get_vm_config():
    """n1-standard-16 VM 설정을 반환합니다."""

    # 1. VM에서 실행될 시작 스크립트 정의
    # 이 스크립트가 MLOps 코드를 다운로드하고 실행합니다.
    startup_script = f"""
    #!/bin/bash

    # 네트워크 연결 대기
    until curl -s -f --connect-timeout 1 http://metadata.google.internal; do
        echo "Waiting for network..."
        sleep 2
    done
    
    # 작업 완료 후 VM 자체 종료 및 삭제
    gcloud compute instances delete {VM_NAME} --zone={ZONE} --quiet 
    """

    config = {
        "name": VM_NAME,
        "machineType": f"zones/{ZONE}/machineTypes/{VM_MACHINE_TYPE}",
        "disks": [
            # ... 디스크 설정 ...
        ],
        "networkInterfaces": [
            # ... 네트워크 설정 ...
        ],
        "metadata": {"items": [{"key": "startup-script", "value": startup_script}]},
    }
    return config


def run_vm_workflow():
    """VM 생성 및 삭제를 제어하는 메인 워크플로."""
    compute = googleapiclient.discovery.build("compute", "v1")

    try:
        # 1. VM 생성 및 startup script 실행
        print(f"VM {VM_NAME} 생성 시작")
        compute.instances().insert(
            project=PROJECT_ID, zone=ZONE, body=get_vm_config()
        ).execute()

        # 2. VM의 작업이 완료될 때까지 대기
        print("MLOps 작업 완료 및 VM 삭제 대기 중 (최대 1시간 가정)")
        for _ in range(60):  # 60분 (60 * 60초) 대기
            time.sleep(60)
            try:
                # VM이 존재하는지 확인 (삭제되면 예외 발생)
                compute.instances().get(
                    project=PROJECT_ID, zone=ZONE, instance=VM_NAME
                ).execute()
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 404:
                    print(f"VM {VM_NAME}이 삭제되었음을 확인했습니다. 작업 완료.")
                    return  # 작업 성공적으로 완료
                raise e

        # 이 부분에 도달하면 시간 초과
        raise TimeoutError(f"VM {VM_NAME}이 60분 내에 작업을 완료하지 못했습니다.")

    finally:
        # VM이 만약 남아있다면 강제 삭제 (보험용)
        try:
            compute.instances().delete(
                project=PROJECT_ID, zone=ZONE, instance=VM_NAME
            ).execute()
            print(f"FINALLY 블록에서 VM {VM_NAME} 강제 삭제 시작.")
        except:
            pass  # 이미 삭제되었거나 다른 오류 발생 시 무시


if __name__ == "__main__":
    run_vm_workflow()

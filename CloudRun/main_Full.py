import time

import googleapiclient.discovery
import googleapiclient.errors

PROJECT_ID = "long-centaur-402106"
ZONE = "us-central1-a"  # VM을 생성할 GCP Zone
VM_MACHINE_TYPE = "n1-standard-4"
VM_NAME = "fullpipeline-vm"
# # 실제 MLOps 코드가 담긴 GCS 버킷 및 파일 경로
GCS_BUCKET = "snp-project-bucket"
# MLOPS_SCRIPT_PATH = "run_mlops.sh"
STARTUP_SCRIPT = f"""#!/bin/bash
set -x
until curl -s -f --connect-timeout 1 http://metadata.google.internal; do
    echo "Waiting for network..."
    sleep 2
done
echo "Connected to metadata server."
sleep 20
gcloud compute instances delete {VM_NAME} --zone={ZONE} --quiet 
"""


def get_vm_config():
    """n1-standard-16 VM 설정을 반환합니다."""

    config = {
        "name": VM_NAME,
        "machineType": f"zones/{ZONE}/machineTypes/{VM_MACHINE_TYPE}",
        "disks": [
            {
                "boot": True,
                "autoDelete": True,
                "initializeParams": {
                    "sourceImage": "projects/debian-cloud/global/images/family/debian-12",
                },
            }
        ],
        "networkInterfaces": [
            {
                "network": "global/networks/default",
            }
        ],
        "serviceAccounts": [
            {
                "email": "1047168964134-compute@developer.gserviceaccount.com",
                "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
            }
        ],
        "metadata": {"items": [{"key": "startup-script", "value": STARTUP_SCRIPT}]},
    }
    return config


def run_vm_workflow():
    """VM 생성 및 삭제를 제어하는 메인 워크플로."""
    compute = googleapiclient.discovery.build("compute", "v1")

    try:
        # 1. VM 생성 및 startup script 실행
        print(f"VM {VM_NAME} 생성 및 코드 실행 시작")
        compute.instances().insert(
            project=PROJECT_ID, zone=ZONE, body=get_vm_config()
        ).execute()

        # 2. VM의 작업이 완료될 때까지 대기
        for i in range(5):  # 5분 (5 * 60초) 대기
            time.sleep(60)
            try:
                # VM이 존재하는지 확인 (삭제되면 예외 발생)
                compute.instances().get(
                    project=PROJECT_ID, zone=ZONE, instance=VM_NAME
                ).execute()
                print(f"VM {VM_NAME}이 아직 실행 중입니다... ({i+1}분 경과)")
            except googleapiclient.errors.HttpError as e:
                if e.resp.status == 404:
                    print(f"VM {VM_NAME}이 삭제되었음을 확인했습니다. 작업 완료.")
                    return  # 작업 성공적으로 완료
                raise e

        # 이 부분에 도달하면 시간 초과
        raise TimeoutError(f"VM {VM_NAME}이 시간 내에 작업을 완료하지 못했습니다.")

    finally:
        # VM이 만약 남아있다면 강제 삭제 (보험용)
        try:
            print(f"FINALLY 블록에서 VM {VM_NAME} 강제 삭제 시작.")
            compute.instances().delete(
                project=PROJECT_ID, zone=ZONE, instance=VM_NAME
            ).execute()
        except:
            pass  # 이미 삭제되었거나 다른 오류 발생 시 무시


if __name__ == "__main__":
    run_vm_workflow()

# ==============================================================================
#   Orchestrator for Student Submission Evaluation
# ==============================================================================
#
#   설명:
#   이 스크립트는 여러 단계의 외부 도구를 사용하여 학생들의 프로그래밍 과제 제출물을
#   자동으로 평가하는 파이프라인 오케스트레이터입니다. 각 학생의 제출물에 대해
#   복호화, 코드 복원, 원본과의 일치도 검사, 개발 과정 분석을 병렬로 수행하고,
#   최종 결과를 하나의 CSV 리포트로 집계합니다.
#
#   주요 기능:
#   - 설정 외부화: 모든 경로와 파일 이름은 `config.json` 파일을 통해 관리됩니다.
#   - 모듈식 설계: 각 기능(도구 실행, 학생 처리)이 함수로 명확하게 분리되어 있습니다.
#   - 병렬 처리: `ThreadPoolExecutor`를 사용하여 다수의 학생을 동시에 처리하여 실행 시간을 단축합니다.
#   - 체계적인 로깅: 모든 실행 과정은 콘솔과 `orchestrator.log` 파일에 기록됩니다.
#
#   사용법:
#   1. 필수 파일 및 폴더 구조 확인:
#      - /project_root
#        - /student_submission
#          - /student-01
#          - /student-02
#          ...
#        - /tools
#          - /mission-decoder
#          - /duplicate_finder
#          ... (모든 필요 도구)
#        - /work  <- (현재 스크립트 위치)
#          - orchestrator.py (이 파일)
#          - config.json (설정 파일)
#
#   2. 설정 파일 확인:
#      - `config.json` 파일의 내용이 실제 프로젝트 구조와 일치하는지 확인합니다.
#
#   3. Poetry 기반 도구 준비:
#      - 스크립트가 처음 실행될 때 'mission-decoder', 'mission-restore' 등의
#        Poetry 프로젝트에 대해 자동으로 `poetry install`을 실행하여 의존성을 설치합니다.
#
#   4. 스크립트 실행:
#      - 터미널에서 `work` 폴더로 이동한 후, 아래 명령어를 실행합니다.
#      - `duration` 인자는 필수이며, 'inspector' 과정 분석 도구가 사용할 시험 시간을 분 단위로 지정합니다.
#      $ python orchestrator.py --duration 60
#
# ==============================================================================

import subprocess
import sys
import os
import csv
import re
import argparse
import json
import logging
import concurrent.futures
from pathlib import Path
from typing import List, Optional, Dict
from tqdm import tqdm

# ==============================================================================
#   헬퍼 함수 (Helper Functions)
# ==============================================================================
# 이 섹션의 함수들은 외부 프로세스를 실행하고 그 결과를 처리하는 저수준(low-level) 작업을 담당합니다.

def ensure_poetry_project_ready(project_path: Path) -> bool:
    """
    주어진 Poetry 프로젝트의 의존성을 확인하고 필요한 경우 'poetry install'을 실행합니다.
    스크립트의 주 로직이 도구 실행에만 집중할 수 있도록 사전 준비 작업을 분리합니다.
    :param project_path: 확인할 Poetry 프로젝트의 경로
    :return: 준비 성공 시 True, 실패 시 False
    """
    project_name = project_path.name
    logging.info(f"'{project_name}' 프로젝트 의존성 확인 및 준비 중...")
    if not (project_path / "pyproject.toml").exists():
        logging.error(f"'{project_path}' 폴더에서 'pyproject.toml' 파일을 찾을 수 없습니다.")
        return False
    
    command = ["poetry", "install"]
    logging.info(f"  실행: {' '.join(command)} (최초 실행 시 시간이 걸릴 수 있습니다)...")
    try:
        # check=True: 명령어가 0이 아닌 종료 코드를 반환하면 CalledProcessError 예외 발생
        # cwd=project_path: 명령어 실행 위치를 해당 프로젝트 폴더로 지정
        subprocess.run(
            command, check=True, cwd=project_path,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE  # 성공/실패 시의 출력은 로깅에서 처리하므로 숨김
        )
        logging.info(f"'{project_name}' 의존성 준비 완료.")
        return True
    except FileNotFoundError:
        logging.error("'poetry' 명령어를 찾을 수 없습니다. 시스템에 Poetry가 설치되어 있고 PATH에 등록되었는지 확인하세요.")
        return False
    except subprocess.CalledProcessError as e:
        # poetry install 실패 시, poetry가 stderr로 출력한 오류 메시지를 로그에 기록
        logging.error(f"'{project_name}' 프로젝트에서 'poetry install' 실행에 실패했습니다.\n{e.stderr.decode()}")
        return False

def run_poetry_project(project_path: Path, module_name: str, args: Optional[List[str]] = None, base_display_path: Optional[Path] = None) -> bool:
    """
    지정된 Poetry 프로젝트의 가상환경에 설치된 Python으로 특정 모듈을 실행합니다.
    'poetry run' 대신 가상환경의 python 실행 파일을 직접 찾아 실행하여 더 명시적이고 안정적입니다.
    :param project_path: 실행할 Poetry 프로젝트 경로
    :param module_name: 실행할 모듈 이름 (예: 'mission_decoder.main')
    :param args: 모듈에 전달할 커맨드 라인 인자 리스트
    :param base_display_path: 로그에 경로를 출력할 때 사용할 기준 경로 (상대 경로로 예쁘게 출력하기 위함)
    :return: 실행 성공 시 True, 실패 시 False
    """
    # ... (내부 로직은 이전과 동일하며 충분히 명확) ...
    project_name = project_path.name
    logging.info(f"Poetry 프로젝트 실행: {project_name}")
    safe_args = args or []
    def to_relative_str(path_to_convert):
        path = Path(path_to_convert)
        if base_display_path and path.is_absolute():
            try: return str(path.relative_to(base_display_path.resolve()))
            except ValueError: return str(path)
        return str(path)
    try:
        venv_path_result = subprocess.run(["poetry", "env", "info", "-p"], cwd=project_path, capture_output=True, text=True, check=True)
        venv_path = Path(venv_path_result.stdout.strip())
        python_executable = (venv_path / "Scripts" / "python.exe") if sys.platform == "win32" else (venv_path / "bin" / "python")
        if not python_executable.exists():
            logging.error(f"가상환경에서 Python 실행 파일을 찾을 수 없습니다: {to_relative_str(python_executable)}")
            return False
        
        command = [str(python_executable), "-m", module_name] + safe_args
        logging.info(f"  실행 명령어: [venv: {project_name}] python -m {module_name}")
        arg_labels = ["  * 입력", "  * 출력", "  * 키"]
        if "decoder" in module_name: arg_labels = ["  * 대상 파일", "  * 개인 키", "  * 출력 파일"]
        for i, arg in enumerate(safe_args):
            label = arg_labels[i] if i < len(arg_labels) else f"  * 인자[{i+1}]"
            arg_path = (project_path / arg).resolve() if not Path(arg).is_absolute() else Path(arg)
            logging.info(f"{label}: {to_relative_str(arg_path)}")
        logging.info(f"  실행 위치: {to_relative_str(project_path)}")
        
        subprocess.run(command, check=True, cwd=project_path, capture_output=True, text=True)
        logging.info(f"완료: {project_name}")
        return True
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        error_message = getattr(e, 'stderr', str(e))
        logging.error(f"'{project_name}' 실행 중 문제 발생:\n{error_message}")
        return False

# ... (run_plain_python_script, run_executable, parse_signature_file 함수들은 유사하게 잘 구조화되어 있음) ...

def run_plain_python_script(script_path: Path, args: Optional[List[str]] = None, base_display_path: Optional[Path] = None) -> Optional[subprocess.CompletedProcess]:
    """일반 Python 스크립트를 현재 시스템의 Python으로 실행하고, 그 실행 결과(CompletedProcess)를 반환합니다."""
    script_name = script_path.name
    logging.info(f"Python 스크립트 실행: {script_name}")
    safe_args = args or []
    command = [sys.executable, str(script_path)] + safe_args
    def to_relative_str(path_to_convert):
        path = Path(path_to_convert)
        if base_display_path and path.is_absolute():
            try: return str(path.relative_to(base_display_path.resolve()))
            except ValueError: return str(path)
        return str(path)
    logging.info(f"  실행 명령어: python {to_relative_str(script_path)}")
    if len(safe_args) >= 2:
        logging.info(f"  * 파일 A: {to_relative_str(safe_args[0])}")
        logging.info(f"  * 파일 B: {to_relative_str(safe_args[1])}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logging.info(f"완료: {script_name}")
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"'{script_name}' 실행 중 문제 발생:\n{e.stderr}")
        return None

def run_executable(
    executable_path: Path, 
    positional_args: Optional[List[str]] = None, # 수정: List[str] 또는 None 허용
    named_args: Optional[Dict[str, str]] = None, # 수정: Dict[str, str] 또는 None 허용
    base_display_path: Optional[Path] = None,
) -> Optional[subprocess.CompletedProcess]:
    """컴파일된 실행 파일을 위치 및 이름 기반 인자로 실행하고, 결과를 반환합니다."""
    exe_name = executable_path.name
    logging.info(f"실행 파일 실행: {exe_name}")
    pos_args = positional_args or []
    named_args_list = []
    if named_args:
        for key, value in named_args.items():
            named_args_list.extend([f"--{key}", str(value)])
    command = [str(executable_path)] + named_args_list + pos_args
    execution_directory = executable_path.parent
    def to_relative_str(path_to_convert):
        path = Path(path_to_convert)
        if base_display_path and path.is_absolute():
            try: return str(path.relative_to(base_display_path.resolve()))
            except ValueError: return str(path)
        return str(path)
    logging.info(f"  실행 명령어: {to_relative_str(executable_path)}")
    logging.info(f"  실행 위치: {to_relative_str(execution_directory)}")
    if named_args:
        for key, value in named_args.items():
            abs_value_path = Path(value) if Path(value).is_absolute() else (execution_directory / value)
            logging.info(f"  * --{key}: {to_relative_str(abs_value_path)}")
    for i, arg in enumerate(pos_args):
        abs_arg_path = Path(arg) if Path(arg).is_absolute() else (execution_directory / arg)
        logging.info(f"  * 위치 인자 #{i+1}: {to_relative_str(abs_arg_path)}")
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True, encoding='utf-8', cwd=execution_directory)
        logging.info(f"완료: {exe_name}")
        return result
    except FileNotFoundError:
        logging.error(f"실행 파일을 찾을 수 없습니다: {executable_path}")
        return None
    except subprocess.CalledProcessError as e:
        logging.error(f"'{exe_name}' 실행 중 문제 발생:\n{e.stderr}")
        return None

def parse_signature_file(filepath: Path) -> str:
    """signature.decrypted 파일을 안전하게 파싱하여 'city' 정보를 추출합니다."""
    logging.info(f"서명 파일 분석: {filepath.name}")
    if not filepath.exists():
        logging.warning(f"서명 파일을 찾을 수 없음: {filepath}")
        return "FILE_NOT_FOUND"
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        # .get()을 연쇄적으로 사용하여 키가 없더라도 KeyError 없이 안전하게 접근
        city = data.get("location_info", {}).get("city")
        if city and isinstance(city, str):
            logging.info(f"  추출된 도시: {city}")
            return city
        else:
            logging.warning(f"'city' 키를 찾을 수 없거나 형식이 올바르지 않음: {filepath}")
            return "KEY_OR_TYPE_INVALID"
    except json.JSONDecodeError:
        logging.error(f"JSON 파싱 오류: {filepath}")
        return "JSON_DECODE_ERROR"
    except Exception as e:
        logging.error(f"알 수 없는 파싱 오류: {filepath} ({e})")
        return "UNKNOWN_PARSING_ERROR"

# ==============================================================================
#   핵심 파이프라인 함수 (Core Pipeline Function)
# ==============================================================================
def process_student_submission(
    student_dir: Path, 
    paths: Dict[str, Path],
    config: Dict,
    cli_args: argparse.Namespace, 
    duplication_map: Dict[str, str]
) -> Dict:
    """
    한 학생의 제출물에 대한 전체 처리 파이프라인을 실행합니다.
    이 함수는 독립적으로 실행 가능하며, 병렬 처리를 위해 스레드 풀의 작업 단위로 사용됩니다.
    :return: CSV 리포트에 기록될 한 행의 데이터 (딕셔셔리 형태)
    """
    student_id = student_dir.name
    logging.info(f"📂 처리 시작: {student_id}")
    
    # 각 단계의 결과를 저장할 변수 초기화
    pipeline_status = "OK"  # 파이프라인의 전반적인 성공/실패 상태
    comparison_result = "NOT_CHECKED"
    analysis_score = "NOT_ANALYZED"
    
    # 이 학생의 출력 파일들이 저장될 개별 폴더 생성 (예: output/processed_outputs/student-01/)
    student_output_dir_abs = paths['processed_base_dir'] / student_id
    student_output_dir_abs.mkdir(exist_ok=True)
    
    # config.json에 정의된 구조를 바탕으로 필요한 모든 입/출력 파일의 절대 경로를 동적으로 구성
    sfs, ofs = config['student_file_structure'], config['output_files']
    original_main_py_path = student_dir / sfs['original_main_py']
    log_dir_abs = student_dir / sfs['log_dir']
    log_encrypted_path_abs = log_dir_abs / sfs['log_encrypted']
    signature_encrypted_path_abs = log_dir_abs / sfs['signature_encrypted']
    log_decrypted_path_abs = student_output_dir_abs / ofs['log_decrypted']
    signature_decrypted_path_abs = student_output_dir_abs / ofs['signature_decrypted']
    log_restored_path_abs = student_output_dir_abs / ofs['log_restored']

    # --- STEP A: 복호화 ---
    logging.info(f"--- {student_id}: 단계 A (복호화) ---")
    files_to_decrypt = [(log_encrypted_path_abs, log_decrypted_path_abs), (signature_encrypted_path_abs, signature_decrypted_path_abs)]
    decryption_ok = True
    for input_path, output_path in files_to_decrypt:
        if not input_path.exists():
            logging.warning(f"{student_id}: 건너뛰기 - {input_path.name} 파일 없음")
            continue
        decoder_args = [os.path.relpath(p, start=paths['decoder_project_path']) for p in [input_path, paths['private_key_path'], output_path]]
        if not run_poetry_project(paths['decoder_project_path'], "mission_decoder.main", decoder_args, paths['root_dir']):
            decryption_ok = False; break
    if not decryption_ok: pipeline_status = "DECRYPT_FAILED"

    # --- STEP B: 코드 복원 ---
    # 이전 단계(복호화)가 실패했다면 이 단계를 건너뛴다.
    if pipeline_status == "OK":
        logging.info(f"--- {student_id}: 단계 B (코드 복원) ---")
        if not log_decrypted_path_abs.exists(): pipeline_status = "RESTORE_FAILED"
        else:
            restore_args = [os.path.relpath(p, start=paths['restore_project_path']) for p in [log_decrypted_path_abs, log_restored_path_abs]]
            if not run_poetry_project(paths['restore_project_path'], "mission_restore.main", restore_args, paths['root_dir']): pipeline_status = "RESTORE_FAILED"

    # --- STEP C: 일치도 검사 ---
    if pipeline_status == "OK":
        logging.info(f"--- {student_id}: 단계 C (일치도 검사) ---")
        if not original_main_py_path.exists() or not log_restored_path_abs.exists(): comparison_result = "FILE_MISSING"
        else:
            # UnboundLocalError를 방지하기 위해 변수 정의와 사용을 명확히 분리
            diff_args = [str(original_main_py_path), str(log_restored_path_abs)]
            diff_result = run_plain_python_script(paths['diff_script_path'], diff_args, paths['root_dir'])
            if diff_result and "✅ 파일이 실질적으로 동일합니다" in diff_result.stdout: comparison_result = "OK"
            elif diff_result: comparison_result = "DIFFERENT"
            else: comparison_result = "CHECK_FAILED"

    # --- STEP D: 과정 분석 ---
    if pipeline_status == "OK":
        logging.info(f"--- {student_id}: 단계 D (과정 분석) ---")
        html_report_path_abs = student_output_dir_abs / ofs['inspection_report_html']
        if not paths['inspector_exe_path'].exists(): analysis_score = "ANALYZER_MISSING"
        elif not log_decrypted_path_abs.exists(): analysis_score = "FILE_MISSING"
        else:
            inspector_named_args = {
                "logfile": os.path.relpath(log_decrypted_path_abs, start=paths['inspector_exe_path'].parent),
                "duration": str(cli_args.duration),
                "html-output": os.path.relpath(html_report_path_abs, start=paths['inspector_exe_path'].parent)
            }
            inspector_result = run_executable(paths['inspector_exe_path'], named_args=inspector_named_args, base_display_path=paths['root_dir'])
            if inspector_result:
                score_pattern = re.compile(r"⏺︎ 최종 앙상블 점수: (\d+) / 100")
                match = score_pattern.search(inspector_result.stdout)
                analysis_score = int(match.group(1)) if match else "SCORE_NOT_FOUND"
            else: analysis_score = "ANALYSIS_FAILED"
    
    # --- 최종 결과 집계 ---
    location_info = parse_signature_file(signature_decrypted_path_abs)
    # 파이프라인이 중간에 실패했다면 그 상태를, 성공했다면 일치도 검사 결과를 최종 상태로 결정
    final_status = comparison_result if pipeline_status == "OK" else pipeline_status
    duplication_group = duplication_map.get(student_id, 'UNIQUE')
    
    logging.info(f"🏁 처리 완료: {student_id}, 최종 상태: {final_status}")
    
    # 최종 리포트의 한 행이 될 딕셔너리 반환
    return {
        'student_id': student_id, 'status': final_status, 
        'duplication_group': duplication_group, 
        'process_analysis_score': analysis_score, 'location': location_info
    }

# ==============================================================================
#   메인 실행 로직 (Main Execution Logic)
# ==============================================================================
def main():
    """스크립트의 메인 진입점. 전체 오케스트레이션 로직을 포함합니다."""
    # 1. 로깅 설정: 모든 로그는 콘솔과 파일에 기록됨
    # [%(threadName)s]을 추가하여 병렬 처리 시 어떤 스레드가 로그를 남겼는지 확인 용이
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s [%(levelname)s] [%(threadName)s] - %(message)s', 
        handlers=[logging.FileHandler("orchestrator.log", mode='w', encoding='utf-8'), logging.StreamHandler(sys.stdout)]
    )
    
    # 2. 커맨드 라인 인자 파싱
    parser = argparse.ArgumentParser(description="Full pipeline orchestrator for student submissions.")
    parser.add_argument('--duration', type=int, required=True, help="과정 분석(inspector)을 위한 시험 시간(분 단위, 필수)")
    cli_args = parser.parse_args()
    
    logging.info("🚀 전체 파이프라인 오케스트레이터 시작...")
    logging.info(f"Inspector 분석 시간: {cli_args.duration}분")

    # 3. 설정 파일(config.json) 로드 및 경로 구성
    # {기존 토드}}
    # work_dir = Path(__file__).resolve().parent
    # {수정}
    # __file__ 대신 현재 작업 디렉토리를 기준으로 경로 설정
    # poetry run은 항상 프로젝트 루트(work/)에서 실행되므로 os.getcwd()가 안정적임
    work_dir = Path(os.getcwd()) 
    root_dir = work_dir.parent
    try:
        with open(work_dir / "config.json", 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        logging.critical("설정 파일(config.json)을 찾을 수 없습니다. 스크립트와 같은 위치에 있는지 확인하세요."); sys.exit(1)
    except json.JSONDecodeError:
        logging.critical("설정 파일(config.json)의 형식이 올바르지 않습니다."); sys.exit(1)
    
    # config 파일과 기본 경로를 조합하여 필요한 모든 절대 경로를 동적으로 생성
    dirs, tools = config['directories'], config['tools']
    student_submission_dir = root_dir / dirs['student_submission']
    tools_dir = root_dir / dirs['tools']
    processed_base_dir = work_dir / dirs['output_base'] / dirs['processed_outputs']
    report_dir = work_dir / dirs['output_base'] / dirs['report']
    for d in [processed_base_dir, report_dir]: d.mkdir(parents=True, exist_ok=True)
    
    # `process_student_submission` 함수에 전달할 경로 묶음(paths 딕셔너리) 생성
    paths = {
        "root_dir": root_dir, "processed_base_dir": processed_base_dir,
        "decoder_project_path": tools_dir / tools['decoder_project'], "restore_project_path": tools_dir / tools['restore_project'],
        "private_key_path": tools_dir / tools['private_key'], "diff_script_path": tools_dir / tools['diff_script'],
        "inspector_exe_path": tools_dir / tools['inspector'],
        "duplicate_finder_exe_path": tools_dir / tools['duplicate_finder'],
    }
    
    # 4. 최종 리포트 파일 초기화 (헤더 작성)
    report_csv_path = report_dir / config['output_files']['report_csv']
    report_headers = ['student_id', 'status', 'duplication_group', 'process_analysis_score', 'location']
    try:
        with open(report_csv_path, 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(report_headers)
        logging.info(f"리포트 파일 위치: {report_csv_path.relative_to(root_dir)}")
    except IOError as e:
        logging.critical(f"리포트 파일을 생성할 수 없습니다: {e}"); sys.exit(1)
    
    # 5. Poetry 기반 도구 의존성 준비
    logging.info("=" * 60)
    logging.info("모든 도구의 의존성 확인 및 준비를 시작합니다...")
    tool_paths_to_prepare = [paths['decoder_project_path'], paths['restore_project_path']]
    if not all(ensure_poetry_project_ready(p) for p in tool_paths_to_prepare):
        logging.critical("하나 이상의 도구를 준비하는 데 실패했습니다. 프로그램을 중단합니다."); sys.exit(1)

    # 6. 사전 분석 (중복 파일 검사)
    logging.info("=" * 60)
    logging.info("사전 분석: 중복된 암호화 로그 파일(.encrypted)을 검색합니다...")
    duplication_map: Dict[str, str] = {}
    if not paths['duplicate_finder_exe_path'].exists():
        logging.warning("중복 검사 도구(duplicate_finder)를 찾을 수 없어 해당 단계를 건너뜁니다.")
    else:
        dup_named_args = {"root-folder": str(student_submission_dir), "file-filter": config['student_file_structure']['log_encrypted']}
        dup_result = run_executable(paths['duplicate_finder_exe_path'], named_args=dup_named_args, base_display_path=root_dir)
        if dup_result:
            group_counter, current_group_id = 0, ''
            pattern = f"{re.escape(dirs['student_submission'])}[\\\\/](student-[^\\\\/]+)"
            student_id_pattern = re.compile(pattern)
            for line in dup_result.stdout.strip().split('\n'):
                if line.startswith("---"):
                    group_counter += 1
                    current_group_id = chr(ord('A') + group_counter - 1)
                elif " - " in line and (match := student_id_pattern.search(line)):
                    duplication_map[match.group(1)] = current_group_id
            logging.info(f"{group_counter}개의 중복 그룹을 찾았습니다.")
            
    # 7. 메인 파이프라인 실행 (병렬 처리)
    logging.info("=" * 60)
    logging.info("개별 학생 제출물에 대한 병렬 파이프라인 처리를 시작합니다...")
    student_dirs = sorted([d for d in student_submission_dir.iterdir() if d.is_dir()])
    report_data = []

    # ThreadPoolExecutor를 사용하여 학생 처리 작업을 병렬로 수행
    # os.cpu_count()를 사용하여 시스템의 코어 수만큼 스레드를 생성 (효율 극대화)
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # 각 학생에 대한 작업을 스레드 풀에 제출. future 객체와 학생 폴더를 매핑하여 추적
        future_to_student = {executor.submit(process_student_submission, sd, paths, config, cli_args, duplication_map): sd for sd in student_dirs}
        
    # [수정] as_completed 루프를 tqdm으로 감싸기
    # total=len(student_dirs): 전체 작업 수를 알려주어 진행률 계산
    # desc="Processing students": 진행률 표시줄 앞에 표시될 텍스트
    for future in tqdm(concurrent.futures.as_completed(future_to_student), total=len(student_dirs), desc="Processing students"):
            student_dir = future_to_student[future]
            try:
                # future.result(): 작업의 반환값(결과 딕셔너리)을 가져옴.
                # 만약 작업 도중 예외가 발생했다면, 이 시점에서 예외가 다시 발생함.
                report_data.append(future.result())
            except Exception:
                # 특정 학생 처리 중 예상치 못한 오류가 발생해도 전체 파이프라인은 멈추지 않음
                logging.error(f"'{student_dir.name}' 처리 중 심각한 예외 발생", exc_info=True)
                report_data.append({'student_id': student_dir.name, 'status': 'PIPELINE_CRASH', 'duplication_group': 'N/A', 'process_analysis_score': 'N/A', 'location': 'N/A'})

    # 8. 최종 리포트 작성
    try:
        with open(report_csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=report_headers)
            # 병렬 처리로 순서가 섞인 결과를 학생 ID 기준으로 정렬하여 보고서의 일관성 유지
            writer.writerows(sorted(report_data, key=lambda item: item['student_id']))
    except IOError as e:
        logging.error(f"리포트 파일에 데이터를 쓰는 데 실패했습니다: {e}")
    
    logging.info("=" * 60)
    logging.info("🎉 모든 파이프라인 작업이 완료되었습니다.")
    logging.info(f"최종 리포트가 저장되었습니다: '{report_csv_path.relative_to(root_dir)}'")


if __name__ == "__main__":
    # 이 스크립트가 직접 실행되었을 때만 main() 함수를 호출
    # (다른 스크립트에서 이 파일을 import 할 때는 main()이 자동 실행되지 않음)
    main()
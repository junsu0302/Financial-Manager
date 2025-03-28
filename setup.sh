#!/bin/bash

# 중복 실행 방지
if [[ -n "$PYTHONPATH_SET" ]]; then
  echo "🔄 PYTHONPATH가 이미 설정되어 있습니다."
  exit 0
fi

# PYTHONPATH 설정
export PYTHONPATH="/Users/junsu/code/Project/Financial-Manager/data:$PYTHONPATH"
export PYTHONPATH_SET=1

# 셸 설정 파일에 추가 (자동 적용)
SHELL_CONFIG_FILE="$HOME/.zshrc"  # zsh 사용 시
# SHELL_CONFIG_FILE="$HOME/.bashrc"  # bash 사용 시 (필요하면 위 줄을 주석 처리하고 이 줄을 사용)

if ! grep -q "PYTHONPATH" "$SHELL_CONFIG_FILE"; then
  echo "export PYTHONPATH=\"/Users/junsu/code/Project/Financial-Manager/data:\$PYTHONPATH\"" >> "$SHELL_CONFIG_FILE"
  echo "✅ PYTHONPATH가 $SHELL_CONFIG_FILE에 저장되었습니다."
else
  echo "🔹 PYTHONPATH가 이미 $SHELL_CONFIG_FILE에 설정되어 있습니다."
fi

echo "✅ PYTHONPATH가 설정되었습니다: $PYTHONPATH"

echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# source setup.sh
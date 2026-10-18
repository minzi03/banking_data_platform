#!/bin/sh
# =============================================================================
# SFTP landing zone seed — chép file mẫu POS/settlement vào thư mục upload.
# =============================================================================
# atmoz/sftp không chạy script này tự động; đây là tiện ích cho dev: chạy tay
# sau khi stack lên để bơm file mẫu vào landing zone (đúng ngày nghiệp vụ).
#
#   docker exec banking-sftp sh /etc/sftp/seed/seed.sh
#
# Trong môi trường thật, file settlement do pipeline đối soát của ngân hàng thả
# vào — script này chỉ mô phỏng bước đó.
set -e

DEST=/home/sftp_user/upload
SRC=/etc/sftp/seed

mkdir -p "$DEST"

# Ngày nghiệp vụ lấy từ tham số (mặc định 2025-12-31 → 20251231).
COB_COMPACT="${1:-20251231}"

cp "$SRC/pos_settlement_sample.csv" "$DEST/pos_settlement_${COB_COMPACT}.csv"
echo "Đã đặt file settlement vào $DEST/pos_settlement_${COB_COMPACT}.csv"
ls -l "$DEST"

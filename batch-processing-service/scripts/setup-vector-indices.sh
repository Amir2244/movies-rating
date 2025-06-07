#!/bin/bash

# Redis Stack Vector Indices Setup Script
# This script creates the necessary vector indices for the batch training service

set -e

# Configuration
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
REDIS_PASSWORD=${REDIS_PASSWORD:-""}

# Vector dimensions (adjust based on your ALS model)
USER_VECTOR_DIM=${USER_VECTOR_DIM:-12}
ITEM_VECTOR_DIM=${ITEM_VECTOR_DIM:-12}

# Distance metric (COSINE, L2, IP)
DISTANCE_METRIC=${DISTANCE_METRIC:-COSINE}

# Index type (FLAT for exact search, HNSW for approximate)
INDEX_TYPE=${INDEX_TYPE:-FLAT}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Redis connection command
redis_cmd() {
    if [ -n "$REDIS_PASSWORD" ]; then
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" "$@"
    else
        redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
    fi
}

# Check if Redis Stack is available
check_redis_stack() {
    log_info "Checking Redis Stack availability..."
    
    if ! command -v redis-cli &> /dev/null; then
        log_error "redis-cli not found. Please install Redis CLI."
        exit 1
    fi
    
    if ! redis_cmd ping > /dev/null 2>&1; then
        log_error "Cannot connect to Redis at $REDIS_HOST:$REDIS_PORT"
        exit 1
    fi
    
    # Check if RedisSearch module is loaded
    if ! redis_cmd MODULE LIST | grep -q "search"; then
        log_error "RedisSearch module not loaded. Please ensure Redis Stack is running."
        exit 1
    fi
    
    log_success "Redis Stack is available and RedisSearch module is loaded"
}

# Drop existing index if it exists
drop_index_if_exists() {
    local index_name=$1
    
    if redis_cmd FT._LIST | grep -q "$index_name"; then
        log_warning "Index $index_name already exists. Dropping..."
        redis_cmd FT.DROPINDEX "$index_name" > /dev/null
        log_info "Dropped existing index: $index_name"
    fi
}

# Create user vector index
create_user_vector_index() {
    local index_name="user_vector_idx"
    
    log_info "Creating user vector index: $index_name"
    drop_index_if_exists "$index_name"
    
    if [ "$INDEX_TYPE" = "FLAT" ]; then
        redis_cmd FT.CREATE "$index_name" \
            ON HASH \
            PREFIX 1 "vector:user:" \
            SCHEMA \
                entity_id NUMERIC SORTABLE \
                entity_type TAG \
                dimension NUMERIC \
                timestamp TEXT \
                vector VECTOR FLAT 6 TYPE FLOAT32 DIM "$USER_VECTOR_DIM" DISTANCE_METRIC "$DISTANCE_METRIC"
    else
        # HNSW index for large datasets
        redis_cmd FT.CREATE "$index_name" \
            ON HASH \
            PREFIX 1 "vector:user:" \
            SCHEMA \
                entity_id NUMERIC SORTABLE \
                entity_type TAG \
                dimension NUMERIC \
                timestamp TEXT \
                vector VECTOR HNSW 14 TYPE FLOAT32 DIM "$USER_VECTOR_DIM" DISTANCE_METRIC "$DISTANCE_METRIC" M 40 EF_CONSTRUCTION 200 EF_RUNTIME 10
    fi
    
    log_success "Created user vector index: $index_name (DIM: $USER_VECTOR_DIM, TYPE: $INDEX_TYPE)"
}

# Create item vector index
create_item_vector_index() {
    local index_name="item_vector_idx"
    
    log_info "Creating item vector index: $index_name"
    drop_index_if_exists "$index_name"
    
    if [ "$INDEX_TYPE" = "FLAT" ]; then
        redis_cmd FT.CREATE "$index_name" \
            ON HASH \
            PREFIX 1 "vector:item:" \
            SCHEMA \
                entity_id NUMERIC SORTABLE \
                entity_type TAG \
                dimension NUMERIC \
                timestamp TEXT \
                vector VECTOR FLAT 6 TYPE FLOAT32 DIM "$ITEM_VECTOR_DIM" DISTANCE_METRIC "$DISTANCE_METRIC"
    else
        # HNSW index for large datasets
        redis_cmd FT.CREATE "$index_name" \
            ON HASH \
            PREFIX 1 "vector:item:" \
            SCHEMA \
                entity_id NUMERIC SORTABLE \
                entity_type TAG \
                dimension NUMERIC \
                timestamp TEXT \
                vector VECTOR HNSW 14 TYPE FLOAT32 DIM "$ITEM_VECTOR_DIM" DISTANCE_METRIC "$DISTANCE_METRIC" M 40 EF_CONSTRUCTION 200 EF_RUNTIME 10
    fi
    
    log_success "Created item vector index: $index_name (DIM: $ITEM_VECTOR_DIM, TYPE: $INDEX_TYPE)"
}

# Create combined vector index (optional)
create_combined_vector_index() {
    local index_name="combined_vector_idx"
    
    log_info "Creating combined vector index: $index_name"
    drop_index_if_exists "$index_name"
    
    # Use the larger dimension for combined index
    local max_dim=$((USER_VECTOR_DIM > ITEM_VECTOR_DIM ? USER_VECTOR_DIM : ITEM_VECTOR_DIM))
    
    if [ "$INDEX_TYPE" = "FLAT" ]; then
        redis_cmd FT.CREATE "$index_name" \
            ON HASH \
            PREFIX 1 "vector:" \
            SCHEMA \
                entity_id NUMERIC SORTABLE \
                entity_type TAG SORTABLE \
                dimension NUMERIC \
                timestamp TEXT \
                vector VECTOR FLAT 6 TYPE FLOAT32 DIM "$max_dim" DISTANCE_METRIC "$DISTANCE_METRIC"
    else
        # HNSW index for large datasets
        redis_cmd FT.CREATE "$index_name" \
            ON HASH \
            PREFIX 1 "vector:" \
            SCHEMA \
                entity_id NUMERIC SORTABLE \
                entity_type TAG SORTABLE \
                dimension NUMERIC \
                timestamp TEXT \
                vector VECTOR HNSW 14 TYPE FLOAT32 DIM "$max_dim" DISTANCE_METRIC "$DISTANCE_METRIC" M 40 EF_CONSTRUCTION 200 EF_RUNTIME 10
    fi
    
    log_success "Created combined vector index: $index_name (DIM: $max_dim, TYPE: $INDEX_TYPE)"
}

# Verify indices
verify_indices() {
    log_info "Verifying created indices..."
    
    local indices=("user_vector_idx" "item_vector_idx" "combined_vector_idx")
    
    for index in "${indices[@]}"; do
        if redis_cmd FT._LIST | grep -q "$index"; then
            log_success "✓ Index $index is available"
            
            # Show index info
            log_info "Index $index details:"
            redis_cmd FT.INFO "$index" | grep -E "(index_name|num_docs|max_doc_id|num_terms|num_records)" | sed 's/^/  /'
        else
            log_warning "✗ Index $index not found"
        fi
    done
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  --host HOST             Redis host (default: localhost)"
    echo "  --port PORT             Redis port (default: 6379)"
    echo "  --password PASSWORD     Redis password (optional)"
    echo "  --user-dim DIM          User vector dimension (default: 50)"
    echo "  --item-dim DIM          Item vector dimension (default: 50)"
    echo "  --distance METRIC       Distance metric: COSINE, L2, IP (default: COSINE)"
    echo "  --index-type TYPE       Index type: FLAT, HNSW (default: FLAT)"
    echo "  --skip-combined         Skip creating combined index"
    echo ""
    echo "Environment variables:"
    echo "  REDIS_HOST              Redis host"
    echo "  REDIS_PORT              Redis port"
    echo "  REDIS_PASSWORD          Redis password"
    echo "  USER_VECTOR_DIM         User vector dimension"
    echo "  ITEM_VECTOR_DIM         Item vector dimension"
    echo "  DISTANCE_METRIC         Distance metric"
    echo "  INDEX_TYPE              Index type"
}

# Parse command line arguments
SKIP_COMBINED=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        --host)
            REDIS_HOST="$2"
            shift 2
            ;;
        --port)
            REDIS_PORT="$2"
            shift 2
            ;;
        --password)
            REDIS_PASSWORD="$2"
            shift 2
            ;;
        --user-dim)
            USER_VECTOR_DIM="$2"
            shift 2
            ;;
        --item-dim)
            ITEM_VECTOR_DIM="$2"
            shift 2
            ;;
        --distance)
            DISTANCE_METRIC="$2"
            shift 2
            ;;
        --index-type)
            INDEX_TYPE="$2"
            shift 2
            ;;
        --skip-combined)
            SKIP_COMBINED=true
            shift
            ;;
        *)
            log_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Main execution
main() {
    log_info "Starting Redis Stack vector indices setup..."
    log_info "Configuration:"
    log_info "  Redis: $REDIS_HOST:$REDIS_PORT"
    log_info "  User vector dimension: $USER_VECTOR_DIM"
    log_info "  Item vector dimension: $ITEM_VECTOR_DIM"
    log_info "  Distance metric: $DISTANCE_METRIC"
    log_info "  Index type: $INDEX_TYPE"
    
    check_redis_stack
    create_user_vector_index
    create_item_vector_index
    
    if [ "$SKIP_COMBINED" = false ]; then
        create_combined_vector_index
    fi
    
    verify_indices
    
    log_success "Vector indices setup completed successfully!"
    log_info "You can now run your batch training service to populate the vector database."
}

# Run main function
main

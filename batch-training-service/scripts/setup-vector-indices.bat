@echo off
REM Redis Stack Vector Indices Setup Script for Windows
REM This script creates the necessary vector indices for the batch training service

setlocal enabledelayedexpansion

REM Configuration
set REDIS_HOST=localhost
set REDIS_PORT=6379
set REDIS_PASSWORD=

REM Vector dimensions (adjust based on your ALS model)
set USER_VECTOR_DIM=12
set ITEM_VECTOR_DIM=12

REM Distance metric (COSINE, L2, IP)
set DISTANCE_METRIC=COSINE

REM Index type (FLAT for exact search, HNSW for approximate)
set INDEX_TYPE=FLAT

REM Colors for output (Windows doesn't support ANSI colors by default)
set INFO_PREFIX=[INFO]
set SUCCESS_PREFIX=[SUCCESS]
set WARNING_PREFIX=[WARNING]
set ERROR_PREFIX=[ERROR]

echo %INFO_PREFIX% Starting Redis Stack vector indices setup...
echo %INFO_PREFIX% Configuration:
echo %INFO_PREFIX%   Redis: %REDIS_HOST%:%REDIS_PORT%
echo %INFO_PREFIX%   User vector dimension: %USER_VECTOR_DIM%
echo %INFO_PREFIX%   Item vector dimension: %ITEM_VECTOR_DIM%
echo %INFO_PREFIX%   Distance metric: %DISTANCE_METRIC%
echo %INFO_PREFIX%   Index type: %INDEX_TYPE%

REM Check if docker exec -it redis-stack redis-cli is available
where docker exec -it redis-stack redis-cli >nul 2>&1
if %errorlevel% neq 0 (
    echo %ERROR_PREFIX% redis-cli not found. Please install Redis CLI or use Docker.
    echo %INFO_PREFIX% You can run this command instead:
    echo docker exec -it redis-stack redis-cli
    pause
    exit /b 1
)

REM Check Redis Stack availability
echo %INFO_PREFIX% Checking Redis Stack availability...
docker exec -it redis-stack redis-cli -h %REDIS_HOST% -p %REDIS_PORT% ping >nul 2>&1
if %errorlevel% neq 0 (
    echo %ERROR_PREFIX% Cannot connect to Redis at %REDIS_HOST%:%REDIS_PORT%
    echo %INFO_PREFIX% Make sure Redis Stack is running with: docker-compose up -d redis-stack
    pause
    exit /b 1
)

REM Check if RedisSearch module is loaded
docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% MODULE LIST | findstr "search" >nul 2>&1
if %errorlevel% neq 0 (
    echo %ERROR_PREFIX% RedisSearch module not loaded. Please ensure Redis Stack is running.
    pause
    exit /b 1
)

echo %SUCCESS_PREFIX% Redis Stack is available and RedisSearch module is loaded

REM Function to drop index if exists
:drop_index_if_exists
set index_name=%1
docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% FT._LIST | findstr "%index_name%" >nul 2>&1
if %errorlevel% equ 0 (
    echo %WARNING_PREFIX% Index %index_name% already exists. Dropping...
    docker exec -it redis-stack redis-cli -h %REDIS_HOST% -p %REDIS_PORT% FT.DROPINDEX %index_name% >nul 2>&1
    echo %INFO_PREFIX% Dropped existing index: %index_name%
)
goto :eof

REM Create user vector index
echo %INFO_PREFIX% Creating user vector index: user_vector_idx
call :drop_index_if_exists user_vector_idx

if "%INDEX_TYPE%"=="FLAT" (
    docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% FT.CREATE user_vector_idx ON HASH PREFIX 1 "vector:user:" SCHEMA entity_id NUMERIC SORTABLE entity_type TAG dimension NUMERIC timestamp TEXT vector VECTOR FLAT 6 TYPE FLOAT32 DIM %USER_VECTOR_DIM% DISTANCE_METRIC %DISTANCE_METRIC%
) else (
docker exec -it redis-stack redis-cli -h %REDIS_HOST% -p %REDIS_PORT% FT.CREATE user_vector_idx ON HASH PREFIX 1 "vector:user:" SCHEMA entity_id NUMERIC SORTABLE entity_type TAG dimension NUMERIC timestamp TEXT vector VECTOR HNSW 14 TYPE FLOAT32 DIM %USER_VECTOR_DIM% DISTANCE_METRIC %DISTANCE_METRIC% M 40 EF_CONSTRUCTION 200 EF_RUNTIME 10
)

if %errorlevel% equ 0 (
    echo %SUCCESS_PREFIX% Created user vector index: user_vector_idx ^(DIM: %USER_VECTOR_DIM%, TYPE: %INDEX_TYPE%^)
) else (
    echo %ERROR_PREFIX% Failed to create user vector index
)

REM Create item vector index
echo %INFO_PREFIX% Creating item vector index: item_vector_idx
call :drop_index_if_exists item_vector_idx

if "%INDEX_TYPE%"=="FLAT" (
    docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% FT.CREATE item_vector_idx ON HASH PREFIX 1 "vector:item:" SCHEMA entity_id NUMERIC SORTABLE entity_type TAG dimension NUMERIC timestamp TEXT vector VECTOR FLAT 6 TYPE FLOAT32 DIM %ITEM_VECTOR_DIM% DISTANCE_METRIC %DISTANCE_METRIC%
) else (
docker exec -it redis-stack redis-cli -h %REDIS_HOST% -p %REDIS_PORT% FT.CREATE item_vector_idx ON HASH PREFIX 1 "vector:item:" SCHEMA entity_id NUMERIC SORTABLE entity_type TAG dimension NUMERIC timestamp TEXT vector VECTOR HNSW 14 TYPE FLOAT32 DIM %ITEM_VECTOR_DIM% DISTANCE_METRIC %DISTANCE_METRIC% M 40 EF_CONSTRUCTION 200 EF_RUNTIME 10
)

if %errorlevel% equ 0 (
    echo %SUCCESS_PREFIX% Created item vector index: item_vector_idx ^(DIM: %ITEM_VECTOR_DIM%, TYPE: %INDEX_TYPE%^)
) else (
    echo %ERROR_PREFIX% Failed to create item vector index
)

REM Create combined vector index
echo %INFO_PREFIX% Creating combined vector index: combined_vector_idx
call :drop_index_if_exists combined_vector_idx

REM Use the larger dimension for combined index
set /a max_dim=%USER_VECTOR_DIM%
if %ITEM_VECTOR_DIM% gtr %USER_VECTOR_DIM% set /a max_dim=%ITEM_VECTOR_DIM%

if "%INDEX_TYPE%"=="FLAT" (
   docker exec -it redis-stack redis-cli -h %REDIS_HOST% -p %REDIS_PORT% FT.CREATE combined_vector_idx ON HASH PREFIX 1 "vector:" SCHEMA entity_id NUMERIC SORTABLE entity_type TAG SORTABLE dimension NUMERIC timestamp TEXT vector VECTOR FLAT 6 TYPE FLOAT32 DIM %max_dim% DISTANCE_METRIC %DISTANCE_METRIC%
) else (
    docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% FT.CREATE combined_vector_idx ON HASH PREFIX 1 "vector:" SCHEMA entity_id NUMERIC SORTABLE entity_type TAG SORTABLE dimension NUMERIC timestamp TEXT vector VECTOR HNSW 14 TYPE FLOAT32 DIM %max_dim% DISTANCE_METRIC %DISTANCE_METRIC% M 40 EF_CONSTRUCTION 200 EF_RUNTIME 10
)

if %errorlevel% equ 0 (
    echo %SUCCESS_PREFIX% Created combined vector index: combined_vector_idx ^(DIM: %max_dim%, TYPE: %INDEX_TYPE%^)
) else (
    echo %ERROR_PREFIX% Failed to create combined vector index
)

REM Verify indices
echo %INFO_PREFIX% Verifying created indices...

docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% FT._LIST | findstr "user_vector_idx" >nul 2>&1
if %errorlevel% equ 0 (
    echo %SUCCESS_PREFIX% ✓ Index user_vector_idx is available
) else (
    echo %WARNING_PREFIX% ✗ Index user_vector_idx not found
)

docker exec -it redis-stack redis-cli  -h %REDIS_HOST% -p %REDIS_PORT% FT._LIST | findstr "item_vector_idx" >nul 2>&1
if %errorlevel% equ 0 (
    echo %SUCCESS_PREFIX% ✓ Index item_vector_idx is available
) else (
    echo %WARNING_PREFIX% ✗ Index item_vector_idx not found
)

docker exec -it redis-stack redis-cli -h %REDIS_HOST% -p %REDIS_PORT% FT._LIST | findstr "combined_vector_idx" >nul 2>&1
if %errorlevel% equ 0 (
    echo %SUCCESS_PREFIX% ✓ Index combined_vector_idx is available
) else (
    echo %WARNING_PREFIX% ✗ Index combined_vector_idx not found
)

echo.
echo %SUCCESS_PREFIX% Vector indices setup completed successfully!
echo %INFO_PREFIX% You can now run your batch training service to populate the vector database.
echo.
echo %INFO_PREFIX% To access RedisInsight web UI, open: http://localhost:8001
echo %INFO_PREFIX% To test vector search, use the Redis CLI or RedisInsight.
echo.

REM Show example commands
echo %INFO_PREFIX% Example commands to test:
echo   redis-cli FT._LIST
echo   redis-cli FT.INFO user_vector_idx
echo   redis-cli FT.INFO item_vector_idx
echo.

pause

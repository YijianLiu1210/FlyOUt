# DistributedSnapper

## How to run it on local machine

0. Set parameters
- In `Constants.cs`, set `isLocalTest` as `true`, set `userName`
- Update `AWS_credential.txt` file

1. Start Redis container
- `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass`
- `.\RunRedis.ps1`

2. Start the following processes in order
- Start Controller: `dotnet run --project .\SnapperExperimentController true 61 2 SNAPPER true true true true localhost:6379`
- Start Worker 0: `dotnet run --project .\SnapperExperimentWorker true 2 0 SNAPPER localhost localhost:6379`
- Start Worker 1: `dotnet run --project .\SnapperExperimentWorker true 2 1 SNAPPER localhost localhost:6379`

- Start Global Silo: `dotnet run --project .\SnapperSiloHost true true 2 -1 SNAPPER true localhost localhost:6379`
- Start Local Silo 0: `dotnet run --project .\SnapperSiloHost true false 2 0 SNAPPER true localhost localhost:6379`
- Start Local Silo 1: `dotnet run --project .\SnapperSiloHost true false 2 1 SNAPPER true localhost localhost:6379`
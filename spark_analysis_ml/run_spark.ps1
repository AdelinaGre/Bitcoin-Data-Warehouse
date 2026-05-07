param(
    [Parameter(Mandatory = $true)]
    [string]$ScriptPath,

    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ScriptArguments
)

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = Join-Path $Root ".venv\Scripts\python.exe"
$SparkHome = Join-Path $Root ".venv\Lib\site-packages\pyspark"
$SparkSubmit = Join-Path $Root ".venv\Scripts\spark-submit2.cmd"

if (-not (Test-Path $PythonExe)) {
    throw "Python executable not found: $PythonExe"
}

if (-not (Test-Path (Join-Path $SparkHome "jars"))) {
    throw "PySpark jars directory not found: $SparkHome\jars"
}

if (-not (Test-Path $SparkSubmit)) {
    throw "spark-submit2.cmd not found: $SparkSubmit"
}

$env:PYSPARK_PYTHON = $PythonExe
$env:PYSPARK_DRIVER_PYTHON = $PythonExe
$env:SPARK_HOME = $SparkHome

& $SparkSubmit $ScriptPath @ScriptArguments

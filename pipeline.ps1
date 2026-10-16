$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
python "$scriptDir\pipeline.py" $args

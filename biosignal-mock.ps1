$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
python "$scriptDir\mock\biosignal_mock_sim.py" $args

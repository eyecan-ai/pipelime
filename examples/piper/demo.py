from pipelime.pipes.piper import Piper

command = "python /Users/danieledegregorio/work/workspace_eyecan/pipelime/tests/sample_data/piper_commands/fake_detector.py"

print(Piper.piper_command_description(command))

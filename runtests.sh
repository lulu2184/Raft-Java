echo "Running Initial-Election..."
java RaftTest Initial-Election 4444 > out.txt
tail -2 out.txt
echo "Running Re-Election..."
java RaftTest Re-Election 4444 > out.txt
tail -2 out.txt
echo "Running Basic-Agree..."
java RaftTest Basic-Agree 4444 > out.txt
tail -2 out.txt
echo "Running Fail-Agree..."
java RaftTest Fail-Agree 4444 > out.txt
tail -2 out.txt
echo "Running Fail-NoAgree..."
java RaftTest Fail-NoAgree 4444 > out.txt
tail -2 out.txt
echo "Running Rejoin..."
java RaftTest Rejoin 4444 > out.txt
tail -2 out.txt
echo "Running Backup..."
java RaftTest Backup 4444 > out.txt
tail -2 out.txt
echo "Running Count..."
java RaftTest Count 4444 > out.txt
tail -2 out.txt

java RaftTest Initial-Election 4444 > out.txt
echo "Finish Initial-Election"
java RaftTest Re-Election 4444 > out.txt
echo "Finish Re-Election"
java RaftTest Basic-Agree 4444 > out.txt
echo "Finish Basic-Agree"
java RaftTest Fail-Agree 4444 > out.txt
echo "Finish Fail-Agree"
java RaftTest Fail-NoAgree 4444 > out.txt
echo "Finish Fail-NoAgree"
java RaftTest Rejoin 4444 > out.txt
echo "Finish Rejoin"
java RaftTest Backup 4444 > out.txt
echo "Finish Backup"
java RaftTest Count 4444 > out.txt
echo "Finish Count"

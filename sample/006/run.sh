cd $(dirname $0)
tablemerge {ours,base,theirs}.csv > out
# tablemerge ours.csv /dev/null theirs.csv > out
if [ $? -eq 0 ]; then
    mv out out.csv
else
    if [ -f resolve.patch ]; then
        patch -p0 <resolve.patch
        tablemergeresolve out > out.csv
        rm out
    else
        echo "prepare resolve.patch"
        cd -
        exit
    fi
fi
diff -u out.csv expected.csv
cd - >/dev/null

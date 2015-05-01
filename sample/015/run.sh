cd $(dirname $0)
tablemerge --in-place {ours,base,theirs}.csv
if [ $? -eq 0 ]; then
    # echo "merge ok, revert ours.csv from backup"
    cp ours.csv{,.res}
    cp ours.csv{.bk,}
else
    if [ -f resolve.patch ]; then
        patch -p0 <resolve.patch
        tablemergeresolve --in-place ours.csv
        if [ $? -eq 0 ]; then
            echo "resolve ok, revert ours.csv from backup"
            cp ours.csv{,.res}
            cp ours.csv{.bk,}
        else
            echo "resolve failed"
        fi
    else
        echo "prepare resolve.patch"
        cd -
        exit
    fi
fi
diff -u ours.csv.res expected.csv
cd - >/dev/null


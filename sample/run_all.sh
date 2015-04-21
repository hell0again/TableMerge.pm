DIR=$(cd $(dirname $0); pwd)
TESTS=$(cat <<EOL
001
002
003
004
005
006
007
008
009
010
011
012
EOL
)
for T in ${TESTS}; do
    echo "${DIR}/${T}/run.sh"
    ${DIR}/${T}/run.sh
done


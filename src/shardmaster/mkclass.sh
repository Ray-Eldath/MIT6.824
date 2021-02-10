#!/bin/sh

#
# create or update 6.824-golabs-YEAR
#
# if a directory has a file mkclass.ignore, ignore the files listed in that file

umask 2

# update this repository:
CLASSREPO=git@g.csail.mit.edu:6.824-golabs-2021

# include only these src/ directories (e.g. not paxos)
SRCS="mr mrapps main labrpc raft kvraft shardkv shardmaster labgob porcupine models"

SD=$(cd $(dirname $0)/.. && /bin/pwd)
CD=/tmp/golabs.$$
git clone $CLASSREPO $CD || exit 1

mkdir -p $CD/src/

cp $SD/mygo/Makefile $CD/Makefile
(cd $CD/ && git add Makefile 2> /dev/null )

cp $SD/mygo/.check-build $CD/.check-build
(cd $CD/ && git add .check-build 2> /dev/null )

cp $SD/mygo/.gitignore $CD/.gitignore
(cd $CD/ && git add .gitignore 2> /dev/null )

cp $SD/mygo/src/.gitignore $CD/src/.gitignore || exit 1
(cd $CD/src && git add .gitignore)

cp $SD/mygo/src/go.mod $CD/src/go.mod
(cd $CD/ && git add src/go.mod 2> /dev/null )

cp $SD/mygo/src/go.sum $CD/src/go.sum
(cd $CD/ && git add src/go.sum 2> /dev/null )

# for D in `(cd $SD/mygo/src ; ls)`
for D in `echo $SRCS`
do
  mkdir -p $CD/src/$D || exit 1
  (cd $SD/mygo/src/$D
  for F in `ls`
  do
    if [ -s mkclass.ignore ]
    then
      if grep -q $F mkclass.ignore
      then
        I=1
      else
        I=0
      fi
    else
      I=0
    fi
    if [ "$F" = "out" ]
    then
      I=1
    fi
    if [ "$F" = "mkclass.ignore" ]
    then
      I=1
    fi
    if echo $F | grep -q '~$'
    then
      I=1
    fi
    if [ $I -eq 1 ]
    then
      echo "ignore $F"
    else
      $SD/bin/mklab.pl $CD/src/$D $F
      (cd $CD/src/$D && git add $F 2> /dev/null )
    fi
  done)
done

(cd $CD ; git commit -am 'update')

echo "Now, examine and push the repo in $CD"

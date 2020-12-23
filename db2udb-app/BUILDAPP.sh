# U\
# clear

echo -----------------------------------------------------------------------------------------------------------

date
ls -l AppHistInitForClient.cpp

export DB2PATH=/opt/IBM/DB2
export EXTRA_C_FLAGS=
export EXTRA_LFLAG="-Wl,-rpath,$DB2PATH/lib64"
export LIB=lib64

gcc $EXTRA_C_FLAGS -I$DB2PATH/include -c AppHistInitForClient.cpp
gcc $EXTRA_C_FLAGS -I$DB2PATH/include -c DB2Server.cpp
gcc $EXTRA_C_FLAGS -I$DB2PATH/include -c Log.cpp

gcc $EXTRA_C_FLAGS  -lstdc++ -o AppHistInitForClient \
             AppHistInitForClient.o DB2Server.o Log.o \
                 -L$DB2PATH/$LIB -ldb2 

echo -----------------------------------------------------------------------------------------------------------


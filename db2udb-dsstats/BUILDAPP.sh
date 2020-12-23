# U\
clear

export DB2PATH=/opt/IBM/DB2
export EXTRA_C_FLAGS=
export EXTRA_LFLAG="-Wl,-rpath,$DB2PATH/lib64"
export LIB=lib64
export DSAPIPATH=/opt/IBM/InformationServer/Server/DSEngine

gcc $EXTRA_C_FLAGS -I$DB2PATH/include -c DB2Server.cpp
gcc $EXTRA_C_FLAGS -I$DB2PATH/include -c Log.cpp
gcc $EXTRA_C_FLAGS -I$DB2PATH/include -c Utils.cpp
gcc $EXTRA_C_FLAGS -I$DB2PATH/include -I$DSAPIPATH/include -c DsApiPru.cpp
gcc $EXTRA_C_FLAGS -I$DB2PATH/include -I$DSAPIPATH/include -c DSAPI3.CPP

# gcc $EXTRA_C_FLAGS  -lstdc++ -o PreProcHeaderTrailer Main.o DB2Server.o Log.o -L$DB2PATH/$LIB -ldb2
gcc $EXTRA_C_FLAGS  -lstdc++ -o DS_Job_Statistics \
             DSAPI3.o DB2Server.o Log.o DsApiPru.o Utils.o \
                 -L$DB2PATH/$LIB -ldb2 -L$DSAPIPATH/lib -lvmdsapi

# DSAPI3.CPP
# DsApiPru.cpp
# DsApiPru.h


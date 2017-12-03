#!/bin/bash
make(){
  pat="^.*:[0-9]+"
  ccred=$(echo -e "\033[0;31m")
  ccyellow=$(echo -e "\033[0;33m")
  ccend=$(echo -e "\033[0m")
  /usr/bin/make "$@" 2>&1 | sed -E -e "/[Ee]rror[: ]/ s%$pat%$ccred&$ccend%g"
  return ${PIPESTATUS[0]}
}


printf "Testing...\n"
cd build
java -server -ea -Xms1G -Xmx1G -jar experiments_instr.jar test
cd ..
printf "Finished testing\n"

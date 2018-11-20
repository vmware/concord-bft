/*
 * Main.cpp
 *
 *  Created on: Jul 11, 2017
 *      Author: atomescu
 */
#include "threshsign/Configuration.h"

#include "Main.h"

#include "XAssert.h"
#include "Log.h"

using std::endl;

int main(int argc, char *argv[]) {
	logerror << "Error logging is enabled!" << endl;
	logwarn << "Warning logging is enabled!" << endl;
	loginfo << "Info logging is enabled!" << endl;
	logdbg << "Debug logging is enabled!" << endl;
	logtrace << "Trace logging is enabled!" << endl;
	loginfo << endl;
#ifndef NDEBUG
	loginfo << "Assertions are enabled!" << endl;
	assertTrue(true);
	assertFalse(false);
#else
	loginfo << "Assertions are disabled!" << endl;
	assertFalse(true);
	assertTrue(false);
#endif
	logdbg << endl;
    logdbg << "Number of arguments: " << argc << endl;

	std::vector<std::string> args;
	for(int i = 0; i < argc; i++) {
		args.push_back(std::string(argv[i]));
	}
    
    unsigned int seed = static_cast<unsigned int>(time(NULL));
    loginfo << "Randomness seed passed to srand(): " << seed << endl;
    srand(seed);

	// Call application-defined AppMain()
	int rc = AppMain(args);
	loginfo << "Exited gracefully with rc = " << rc << "." << endl;
	return rc;
}

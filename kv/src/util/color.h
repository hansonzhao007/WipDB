#ifndef TERMINAL_COLOR__H_
#define TERMINAL_COLOR__H_

// fprintf(stdout, "[----------] %s\n", debug::HlYellow("Inserted K-V: " + key.ToString() + "-" + val.ToString()));
#include <string>
using std::string;

#define GTEST_COUT std::cerr << debug::Red("[----------] ")

namespace debug {

    class Color {
    public:
        string reset="\e[0m";

        class FG {
        public:
            string black="\e[30m";
            string red="\e[31m";
            string green="\e[32m";
            string orange="\e[33m";
            string blue="\e[34m";
            string purple="\e[35m";
            string cyan="\e[36m";
            string lightgrey="\e[37m";
            string darkgrey="\e[90m";
            string lightred="\e[91m";
            string lightgreen="\e[92m";
            string yellow="\e[93m";
            string lightblue="\e[94m";
            string pink="\e[95m";
            string lightcyan="\e[96m";
        }fg;

        class BG {
        public:
            string black="\e[40m";
            string red="\e[41m";
            string green="\e[42m";
            string orange="\e[43m";
            string blue="\e[44m";
            string purple="\e[45m";
            string cyan="\e[46m";
            string lightgrey="\e[47m";
        }bg; 
    }color; 

    std::string HlYellow(string text) {
        return (color.bg.orange + color.fg.black + text + color.reset);
    }
    
    std::string Yellow(string text) {
        return (color.fg.orange + text + color.reset);
    }
    std::string Red(string text) {
        return (color.fg.red + text + color.reset);
    }
} 
#endif
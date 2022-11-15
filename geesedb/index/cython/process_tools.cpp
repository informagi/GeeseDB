#include <iostream>
#include <string>

#include "english_stem.h"

namespace stemming{
stemming::english_stem<> StemEnglish;

stemming::stemming(string *word){
this->word = word;
}

string stemming::stem () {
    return StemEnglish(this->word);
}

}
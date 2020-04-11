#include <stdlib.h>
#include <ctime>

// Whether the rand() call in rand_port() has been seeded. Used to make sure
// we only seed once.
bool seeded = false;

int rand_port() {
    if (!seeded) {
        srand(time(NULL));
        seeded = true;
    }

    return 4000 + rand() % 3000;
}

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/reboot.h>
#include <errno.h>

int main() {
    // Perform sync operation
    printf("Synchronizing filesystems...\n");
    sync();

    // Sleep for a brief period to ensure sync is completed
    sleep(2);

    // Reboot the system
    printf("Rebooting the system...\n");
    if (reboot(RB_AUTOBOOT) == -1) {
        perror("Reboot failed");
        return errno;
    }

    return 0;
}


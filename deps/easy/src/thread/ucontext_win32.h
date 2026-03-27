/*
 * ucontext compatibility layer for Windows
 * Maps ucontext API to Windows Fibers
 */

#ifndef UCONTEXT_WIN32_H
#define UCONTEXT_WIN32_H

#ifdef _WIN32

#include <windows.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ucontext_t {
    LPVOID fiber;
    void (*uc_func)(void);
    void *uc_stack;
    size_t uc_stacksize;
    struct ucontext_t *uc_link;
    int owns_fiber;
} ucontext_t;

static inline void ensure_fiber() {
    static __declspec(thread) int is_fiber = 0;
    if (!is_fiber) {
        if (!IsThreadAFiber()) {
            LPVOID f = ConvertThreadToFiber(NULL);
            if (!f) {
                return;
            }
        }
        is_fiber = 1;
    }
}

static inline int getcontext(ucontext_t *ucp) {
    if (!ucp) return -1;
    ensure_fiber();
    ucp->fiber = GetCurrentFiber();
    ucp->uc_link = NULL;
    ucp->owns_fiber = 0;
    return 0;
}

static inline int setcontext(const ucontext_t *ucp) {
    if (!ucp || !ucp->fiber) return -1;
    SwitchToFiber(ucp->fiber);
    return 0;
}

static VOID CALLBACK fiber_start_routine(LPVOID lpParameter) {
    ucontext_t *ucp = (ucontext_t*)lpParameter;
    if (ucp && ucp->uc_func) {
        ucp->uc_func();
    }
    if (ucp && ucp->uc_link && ucp->uc_link->fiber) {
        SwitchToFiber(ucp->uc_link->fiber);
    }
}

static inline int makecontext(ucontext_t *ucp, void (*func)(void), int argc, ...) {
    if (!ucp) return -1;

    ensure_fiber();
    ucp->uc_func = func;

    SIZE_T stacksize = ucp->uc_stacksize > 0 ? ucp->uc_stacksize : 65536;
    ucp->fiber = CreateFiber(stacksize, fiber_start_routine, ucp);
    if (!ucp->fiber) return -1;
    ucp->owns_fiber = 1;

    return 0;
}

static inline void freecontext(ucontext_t *ucp) {
    if (ucp && ucp->fiber && ucp->owns_fiber) {
        DeleteFiber(ucp->fiber);
        ucp->fiber = NULL;
        ucp->owns_fiber = 0;
    }
}

static inline int swapcontext(ucontext_t *oucp, const ucontext_t *ucp) {
    if (!oucp || !ucp) return -1;

    ensure_fiber();

    oucp->fiber = GetCurrentFiber();

    if (ucp->fiber) {
        SwitchToFiber(ucp->fiber);
    }

    return 0;
}

#ifdef __cplusplus
}
#endif

#endif // _WIN32
#endif // UCONTEXT_WIN32_H

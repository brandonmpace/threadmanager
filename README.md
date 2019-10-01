# threadmanager
A thread manager for Python programs

##### Current state: Under development (beta - used in production, but new features are planned)


It provides:  
  * Centralization of starting and monitoring threads  
  * State management for the functions in the threads  
  * Logging for exceptions and excessive runtime  
  
Original use case:  
  * GUI program that calls back-end functions for IO-bound work  
  * GUI has a cancel button that should always work, so:  
    * the GUI mainloop should not be blocked  
    * the called functions should intermittently check if the user has pressed the cancel button  
  * Avoid running new work threads when the user wants to cancel  
  * Allow running callback functions when the program starts working or goes idle.  
    * e.g. update a status bar with certain text  
  
Installation:  
  * pip install threadmanager  
  
Tested for Python >=3.6.5 on Linux (Ubuntu) and Windows 7/10

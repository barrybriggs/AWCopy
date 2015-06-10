## Welcome to AWCopy ##


**AWCopy** is an Azure service that provides parallelized copies of S3 files in Amazon Web Services to Azure blobs.

### Building AWCopy ###
AWCopy is:

- A command line tool
- An Azure service which utilizes the [Project Orleans](https://www.github.com/dotnet/orleans) actor framework to parallelize its work
- A very primitive angular.js dashboard

The current version in the initial branch uses the Orleans 0.9 framework (there are slight changes to the syntax for the current 1.0.8 release). This will be updated very shortly. 

The command line tool `awcopy.exe` typically sends commands to a WebAPI running in an Azure web role; these then are forwarded to the actors ("grains") which run in the worker role. There are lots of switches which provide a number of configuration and execution options.

Files sent to the Azure service are divided up among grain instance, 10 files per grain instance (although this is configurable).  

Running in the web role is a very trivial dashboard consisting of an HTML file and a short angular.js file which reads the status from an Azure table. 

This will built with Visual Studio 2013. Project Orleans requires the Azure SDK version 2.4. You'll also need your own Azure subscription and storage account. You'll see in the code various indications like `YOURAZURESTORAGEACCOUNT` where you should fill in your information. Finally, you'll need to ensure that your copies of `OrleansConfiguration.xml` (worker role) and `ClientConfiguration.xml` (web role) are configured for your subscription, as are the Azure service configuration files.  

To build, create a Visual Studio Azure Service solution. Create a console application called `awcopy` and replace the default `program.cs` with the cloned version. Create an Orleans grain implementation project and an Orleans grain interface project, and use the respective code snippets from /Grains and /GrainInterfaces, making the appropriate edits for your service and subscription.  You'll also need an Azure table to store status (in `Globals\Globals.cs` you'll see code for both DocumentDB and Azure tables. Only the latter is used; the former is there for later development). 

### Using AWCopy ###

Command line syntax is

    awcopy arguments switches

where the arguments include AWS parameters and Azure parameters, where appropriate for the switches:

AWS Parameters:

    bucket:{aws-bucket-specification}
    path:{aws-object-path}
    accesskey:{aws-s3-accesskey}
    secret:{aws-s3-secret}
    region:{aws-s3-region}

Azure Parameters:
    
    container:{azure-container-specification}
    storageaccount:{azure-storage-account}
    storagekey:{azure-storage-key}

Switches (subject to change over time) 

    /C: Use the cloud batch service to bulk copy files
    /B: Specify block size (default: 4MB); format nnnnK or nnnnM
    /U: Simple copy (object downloaded locally then uploaded) (default)
    /E: Encrypt blobs stored in Azure
    /J: Job name (does not have to be unique; no spaces; default:"default"
    /D: Delete files in AWS after copy (confirmation required)
    /S: Recursively copy
    /P: Prompt to overwrite existing Azure blob 
    /N: Display number of files to copy only (no actual copy)
    /A: (Maximum) number of agents in Azure to handle copy (default:4)
    /L: List contents of AWS bucket (Azure parameters ignored) with estimated egress charges
    /H: Display help on any command
    /Z: Log all activity to local journal file, format is awcopy-date-time.log
    /V: Verbose logging

VERY IMPORTANT: Please be aware that AWS charges will apply to data copied out of AWS. The /L command which lists the contents of the AWS bucket/path will *estimate* egress charges (at 0.09/GB) but your mileage may vary.  
 
### Known Bugs and Enhancements ###

- Full wildcard support (*.pdf, *.p?f, etc), has not been tested and probably will lead to unexpected results
- Support for encryption on either end
- Config files to replace long command lines




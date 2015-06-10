# AWCopy

This is AWCopy, a tool to transfer S3 files to Azure blobs. It uses Project Orleans (http://www.github.com/dotnet/orleans) to create parallel actors, or grains, to move potentially large numbers of files from S3 to Azure. 

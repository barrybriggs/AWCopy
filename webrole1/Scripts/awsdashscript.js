/********************************************************************************************************
 *  AWCOPY:  COPY S3 OBJECTS TO AZURE BLOBS
 * 
 *  Copyright (c) Barry Briggs
 *  All Rights Reserved 
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the 
 *  License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *  THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, 
 *  INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE, 
 *  MERCHANTABLITY OR NON-INFRINGEMENT. 
 *  
 *  See the Apache 2 License for the specific language governing permissions and limitations under the License. 
 *
 * 
 * 
 * ******************************************************************************************************/
(function () {

    var app = angular.module("test1", []);

    var MainController = function ($scope, $http) {

        var onUserComplete = function (response) {
            $scope.recs = response.data;
        };

        var onError = function (reason) {
            $scope.error = "Could not fetch data";
        };

        $http.get("http://localhost:8080/api/data?tbldata=0")
        //$http.get("http://YOURAZUREURL:8080/api/data?tbldata=0")
          .then(onUserComplete, onError);

        $scope.message = "AWS to Azure Transfer Dashboard"


    };

    app.controller("MainController", ["$scope", "$http", MainController]);

}());
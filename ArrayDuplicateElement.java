package scalawag;

/************************************************************************
 * Author: Tanu Sachdeva
 * Topic: Array (Java) 
 * Problem: Find duplicate elements in an array
*/

public class ArrayDuplicateElement {

	/********************************************************************
	 * Define an integer array 
	 * Call findDuplicate() by passing the array as an argument 
	 * to find duplicate elements
	 */
	
	public static void main(String[] args) {
		int[] arr = new int[]{2,6,3,5,2,6,8};
        System.out.println("Duplicate Elements are : ");
        findDuplicate(arr);
	}
	
	/********************************************************************
	 * Check for duplicate elements and print 
	 * @param - an integer array
	 */
	
    public static void findDuplicate(int[] arr){
	   int i,j;
	   for(i=0;i<arr.length-1;i++){
           for(j=i+1;j<arr.length;j++){
               if(arr[i]==arr[j] && i!=j){
                   System.out.println(arr[i]);
               }
           }
	   }
   }
}

package com.tianya._02动态数组;

public class DynamicArray<E> {
    /**
     * 元素的数量
     */
    private int size;
    /**
     * 所有的元素
     */
    private  E[] elements;

    private static final  int DEFAULT_CAPACITY = 10;

    private static final int ELEMENT_NOT_FOUND = -1;

    public DynamicArray(int capacity){
        //对参数进行判断
        capacity = capacity < DEFAULT_CAPACITY? DEFAULT_CAPACITY:capacity;
        elements = (E[]) new Object[capacity];
    }

    public DynamicArray(){
        this(DEFAULT_CAPACITY);
    }



    /**
     * 清除所有元素
     */
    public void clear(){
        size = 0;
    }

    /**
     * 元素的数量
     * @return
     */
    public int size(){
        return size;
    }

    /**
     * 是否为空
     * @return
     */
    public boolean isEmpty(){
        return size == 0;
    }

    /**
     * 是否包含某个元素
     * @param element
     * @return
     */
    public boolean contains(E element){
        return indexof(element)!= ELEMENT_NOT_FOUND;
    }

    /**
     * 添加元素到尾部
     * @param element
     */
    public void  add(E element){
        add(size,element);
    }

    /**
     * 获取index位置的元素
     * @param index
     * @return
     */
    public E get(int index){
        rangeCheck(index);
        return elements[index];
    }

    /**
     * 设置index位置的元素
     * @param index
     * @param element
     * @return 原来的元素
     */
    public E set(int index,E element){
        rangeCheck(index);
        E old = elements[index];
        elements[index] = element;
        return old;
    }

    /**
     * 在index位置插入一个元素
     * @param index
     * @param element
     */
    public void  add(int index,E element){
        rangeCheckForAdd(index);
        //因为要add一个元素  只需要保证数组的容量为size+1 则一定可以添加一个元素
        ensureCapacity(size+1);
        for (int i = size-1; i >=index ; i--) {
            elements[i+1]=elements[i];
        }
        elements[index] =element;
        size++;

    }

    /**
     * 删除index位置的元素
     * @param index
     * @return
     */
    public E remove(int index){
        rangeCheck(index);
        E old = elements[index];
        for (int i = index+1; i <= size -1 ; i++) {
            elements[i-1] = elements[i];
        }
        size --;
        return old;
    }

    /**
     * 查看元素的索引
     * @param element
     * @return
     */
    public int indexof(E element){
        for (int i = 0; i <size; i++) {
            if(elements[i]==element){return i;}
        }
        return ELEMENT_NOT_FOUND;
    }


    /**
     * 保证要有capacity的容量
     * @param capacity
     */
    private void ensureCapacity(int capacity){
        int oldCapacity = elements.length;
        if(oldCapacity>=capacity){
            return;
        }
        //如果需要扩容  则把新的容量设置为旧容量的1.5倍
        int newCapacity = oldCapacity+ oldCapacity>>1;
        E[] newEelments = (E[])new Object[newCapacity];
        for (int i = 0; i < size; i++) {
            newEelments[i]=elements[i];
        }
        elements = newEelments;
    }


    private void rangeCheck(int index){
        if(index < 0 || index >= size){
            outOfBounds(index);
        }
    }

    private void rangeCheckForAdd(int index){
        if(index < 0 || index > size){
            outOfBounds(index);
        }
    }

    private void outOfBounds(int index){
        throw new IndexOutOfBoundsException("index:"+index + ", Size:"+size);
    }


    // size=3, [99,98,97]
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("size=").append(size).append(", [");
        for (int i = 0; i < size; i++) {
            if(i!=0){
                stringBuilder.append(",");
            }
            stringBuilder.append(elements[i]);
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}

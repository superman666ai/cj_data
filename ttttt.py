#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/7/19 16:48
# @Author  : GaoJian
# @File    : ttttt.py


class Pay_regulations:
    """

    """

class Wechat_pay(Pay_regulations):
    def pay(self,money):
        print('您通过微信支付%s元' %money)

class Alipay(Pay_regulations):
    def pay(self,money):
        print('您通过支付宝支付%s元' %money)

w = Wechat_pay()
w.pay(200)
a = Alipay()
a.pay(100)



class Wechat_pay(Pay_regulations):
    def pay(self, money):
        print('您通过微信支付%s￥' % money)


class Alipay(Pay_regulations):
    def pay(self, money):
        print('您通过支付宝支付%s￥' % money)


def pay(obj, money):
    obj.pay(money)


w = Wechat_pay()
a = Alipay()
pay(w, 100)
pay(a, 200)


class Pay_regulations:   # 定义一个父类，作为规范
    def pay(self,money):
        pass

class Wechat_pay(Pay_regulations):  # 每个接口继承父类的规范
    def pay(self,money):
        print('您通过微信支付%s元' %money)

class Alipay(Pay_regulations):
    def pay(self,money):
        print('您通过支付宝支付%s元' %money)

def pay(obj,money):
    obj.pay(money)

w = Wechat_pay()
a = Alipay()
pay(a,400)
pay(w,300)



from abc import ABCMeta,abstractmethod
# 导入接口类的必要模块，作用：让定义的接口强制按照这个规范去执行，不按规范执行就会报错，这种格式就表示的是抽象类。

class Pay_regulations(metaclass=ABCMeta):
    @abstractmethod
    def pay(self,money):
        pass

class Wechat_pay(Pay_regulations):
    def pay(self,money):
        print('您通过微信支付%s元' %money)

class Alipay(Pay_regulations):
    def pay(self,money):
        print('您通过支付宝支付%s元' %money)

def pay(obj,money):
    obj.pay(money)

w = Wechat_pay()
a = Alipay()
pay(a,400000)
pay(w,300000000)
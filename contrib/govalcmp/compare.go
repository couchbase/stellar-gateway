package govalcmp

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"golang.org/x/exp/constraints"
)

func CompareSimple(left, right SimpleType) (int, error) {
	leftFloat, isLeftFloat := left.(float64)
	rightFloat, isRightFloat := right.(float64)
	leftInt, isLeftInt := left.(int64)
	rightInt, isRightInt := right.(int64)
	leftUint, isLeftUint := left.(uint64)
	rightUint, isRightUint := right.(uint64)
	leftBool, isLeftBool := left.(bool)
	rightBool, isRightBool := right.(bool)
	leftStr, isLeftStr := left.(string)
	rightStr, isRightStr := right.(string)
	leftByteArr, isLeftByteArr := left.([]byte)
	rightByteArr, isRightByteArr := right.([]byte)
	leftJson, isLeftJson := left.(json.RawMessage)
	rightJson, isRightJson := right.(json.RawMessage)

	// Handle matching types (trivial case)
	if isLeftFloat && isRightFloat {
		return compareNumeric(leftFloat, rightFloat), nil
	}
	if isLeftInt && isRightInt {
		return compareNumeric(leftInt, rightInt), nil
	}
	if isLeftUint && isRightUint {
		return compareNumeric(leftUint, rightUint), nil
	}
	if isLeftBool && isRightBool {
		return compareNumeric(boolToUint(leftBool), boolToUint(rightBool)), nil
	}
	if isLeftStr && isRightStr {
		return strings.Compare(leftStr, rightStr), nil
	}
	if isLeftByteArr && isRightByteArr {
		return bytes.Compare(leftByteArr, rightByteArr), nil
	}
	if isLeftJson && isRightJson {
		return compareJson(leftJson, rightJson)
	}

	// perform comparison based on ordered type precedence:
	if isLeftBool || isRightBool {
		bleft, bright, err := coerceLeftRight[bool](left, right)
		if err != nil {
			return 0, err
		}

		return compareNumeric(boolToUint(bleft), boolToUint(bright)), nil
	}

	if isLeftFloat || isRightFloat {
		fleft, fright, err := coerceLeftRight[float64](left, right)
		if err != nil {
			return 0, err
		}

		return compareNumeric(fleft, fright), nil
	}

	if isLeftUint || isRightUint {
		uleft, uright, err := coerceLeftRight[uint64](left, right)
		if err != nil {
			return 0, err
		}

		return compareNumeric(uleft, uright), nil
	}

	if isLeftInt || isRightInt {
		ileft, iright, err := coerceLeftRight[int64](left, right)
		if err != nil {
			return 0, err
		}

		return compareNumeric(ileft, iright), nil
	}

	// in all other cases (string, bytearr, json), we return an error for now because the rules are complicated...

	return 0, errors.New("invalid comparison types")
}

func coerceLeftRight[T SimpleTypeConstaint](left SimpleType, right SimpleType) (T, T, error) {
	var tZero T

	vleft, err := CoerceSimpleToSimple[T](left)
	if err != nil {
		return tZero, tZero, err
	}

	vright, err := CoerceSimpleToSimple[T](right)
	if err != nil {
		return tZero, tZero, err
	}

	return vleft, vright, nil
}

func compareNumeric[T constraints.Ordered](left, right T) int {
	if left < right {
		return -1
	} else if left > right {
		return +1
	}
	return 0
}

func compareJson(left, right json.RawMessage) (int, error) {
	var vleft interface{}
	var vright interface{}

	err := json.Unmarshal(left, &vleft)
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal(right, &vright)
	if err != nil {
		return 0, err
	}

	// JSON comparison performs a deep equality check, then orders the
	// results based on which JSON object is bytewise longer...
	if reflect.DeepEqual(left, right) {
		return 0, nil
	} else {
		if len(left) > len(right) {
			return 1, nil
		} else {
			return -1, nil
		}
	}
}

func Compare(left, right interface{}) (int, error) {
	sleft, err := SimplifyValue(left)
	if err != nil {
		return 0, err
	}

	sright, err := SimplifyValue(right)
	if err != nil {
		return 0, err
	}

	return CompareSimple(sleft, sright)
}
